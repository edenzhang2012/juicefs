/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vfs

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

/*
 * state of sliceReader
 *
 *    <-- REFRESH
 *   |      |
 *  NEW -> BUSY  -> READY
 *          |         |
 *        BREAK ---> INVALID
 */
const (
	NEW     = iota //初始状态
	BUSY           //读取数据中，未完成
	REFRESH        //BUSY状态下，当前Slice被invalid。对应场景可能是：异步读取过程中（可能是预取），写入数据会导致读缓存失效
	BREAK          //调用drop时，若状态未READY且ref==0则进入当前状态
	READY          //读取成功完成
	INVALID        //数据已不再被需要
)

const readSessions = 2

var readBufferUsed int64

type sstate uint8

func (m sstate) valid() bool { return m != BREAK && m != INVALID }

var stateNames = []string{"NEW", "BUSY", "REFRESH", "BREAK", "READY", "INVALID"}

func (m sstate) String() string {
	if m <= INVALID {
		return stateNames[m]
	}
	panic("<unknown>")
}

type FileReader interface {
	Read(ctx meta.Context, off uint64, buf []byte) (int, syscall.Errno)
	GetLength() uint64
	Close(ctx meta.Context)
}

type DataReader interface {
	Open(inode Ino, length uint64) FileReader
	Truncate(inode Ino, length uint64)
	Invalidate(inode Ino, off, length uint64)
}

type frange struct {
	off uint64
	len uint64
}

func (r *frange) String() string { return fmt.Sprintf("[%d,%d,%d)", r.off, r.len, r.end()) }
func (r *frange) end() uint64    { return r.off + r.len }

// p在r范围内
func (r *frange) contain(p uint64) bool { return r.off < p && p < r.end() }

// r与a存在区间重叠
func (r *frange) overlap(a *frange) bool { return a.off < r.end() && r.off < a.end() }

// a完全在r的范围内
func (r *frange) include(a *frange) bool { return r.off <= a.off && a.end() <= r.end() }

// protected by file
type sliceReader struct {
	file       *fileReader   //关联到文件
	block      *frange       //当前Slice的数据off+len，文件视角
	state      sstate        //状态
	page       *chunk.Page   //读缓存数据
	indx       uint32        //chunk index
	currentPos uint32        //当前Slice中的当前读取位置
	lastAccess time.Time     //最后访问时间
	cond       *utils.Cond   //通知
	next       *sliceReader  //Slice链表
	prev       **sliceReader //Slice链表
	refs       uint16        //引用计数
}

func (s *sliceReader) delay(delay time.Duration) {
	time.AfterFunc(delay, s.run)
}

// 标记本次动作完成，更新对应的Slice状态，退出run协程，只会在run中被调用
func (s *sliceReader) done(err syscall.Errno, delay time.Duration) {
	f := s.file
	switch s.state {
	case BUSY:
		s.state = NEW // failed 失败情况下，直接从BUSY转为NEW，重新读取
	case BREAK:
		s.state = INVALID //
	case REFRESH:
		s.state = NEW
	}
	if err != 0 {
		if !f.closing {
			logger.Errorf("read file %d: %s", f.inode, err)
		}
		f.err = err
	}
	if f.shouldStop() {
		s.state = INVALID
	}

	switch s.state {
	case NEW:
		s.delay(delay) //延时后重新读取
	case READY:
		s.cond.Broadcast() //通知数据准备完毕
	case INVALID:
		//缓存淘汰
		if s.refs == 0 {
			s.delete()
			if f.closing && f.slices == nil {
				f.r.Lock()
				if f.refs == 0 {
					f.delete()
				}
				f.r.Unlock()
			}
		} else {
			//还在被引用，通知处理
			s.cond.Broadcast()
		}
	}
	runtime.Goexit() //退出协程
}

func retry_time(trycnt uint32) time.Duration {
	if trycnt < 30 {
		return time.Millisecond * time.Duration((trycnt-1)*300+1)
	}
	return time.Second * 10
}

func (s *sliceReader) run() {
	f := s.file
	f.Lock()
	defer f.Unlock()
	if s.state != NEW || f.shouldStop() {
		s.done(0, 0)
	}
	//开始处理之后状态变更为BUSY
	s.state = BUSY
	indx := s.indx
	inode := f.inode
	f.Unlock()

	var slices []meta.Slice
	//获取chunk下所有的Slice列表
	err := f.r.m.Read(meta.Background(), inode, indx, &slices)
	f.Lock()
	length := f.length
	//状态不是BUSY || 有错误 || 文件close
	if s.state != BUSY || f.err != 0 || f.closing {
		s.done(0, 0)
	}
	if err == syscall.ENOENT {
		//直接通知失败，不重试
		s.done(err, 0)
	} else if err != 0 {
		f.tried++
		trycnt := f.tried
		if trycnt > f.r.maxRetries {
			//超过重试次数，通知失败
			s.done(syscall.EIO, 0)
		} else {
			//重试
			s.done(0, retry_time(trycnt))
		}
	}

	s.currentPos = 0
	//读取的offset超过文件长度,直接标记完成，返回空数据
	if s.block.off > length {
		s.block.len = 0
		s.state = READY
		s.done(0, 0)
	} else if s.block.end() > length { //读取的end超过文件长度，最多只读到文件长度
		s.block.len = length - s.block.off
	}
	need := s.block.len
	f.Unlock()

	//Slice page在外部已经申请完毕
	p := s.page.Slice(0, int(need))
	defer p.Release()
	var n int
	ctx := context.WithValue(context.TODO(), meta.CtxKey("inode"), inode) // Output inode in log for debugging
	//读取数据
	n = f.r.Read(ctx, p, slices, (uint32(s.block.off))%meta.ChunkSize)

	f.Lock()
	if s.state != BUSY || f.shouldStop() {
		s.done(0, 0)
	}
	if n == int(need) { //读到了需要的数据
		s.state = READY
		s.currentPos = uint32(n)
		s.file.tried = 0
		s.lastAccess = time.Now()
		s.done(0, 0)
	} else { //读到的数据长度不对
		s.currentPos = 0 // start again from beginning
		err = syscall.EIO
		f.tried++
		//可能是内存中的chunk缓存不对导致读取到的数据长度不对，无效化内存chunk缓存
		_ = f.r.m.InvalidateChunkCache(meta.Background(), inode, indx)
		if f.tried > f.r.maxRetries {
			s.done(err, 0)
		} else {
			s.done(0, retry_time(f.tried))
		}
	}
}

func (s *sliceReader) invalidate() {
	switch s.state {
	case NEW:
	case BUSY:
		s.state = REFRESH
		// TODO: interrupt reader
	case READY:
		if s.refs > 0 {
			s.state = NEW
			go s.run()
		} else {
			s.state = INVALID
			s.delete() // nobody wants it anymore, so delete it
		}
	}
}

func (s *sliceReader) drop() {
	if s.state <= BREAK {
		if s.refs == 0 {
			s.state = BREAK
			// TODO: interrupt reader
		}
	} else {
		if s.refs == 0 {
			s.delete() // nobody wants it anymore, so delete it
		} else if s.state == READY {
			s.state = INVALID // somebody still using it, so mark it for removal
		}
	}
}

func (s *sliceReader) delete() {
	*(s.prev) = s.next
	if s.next != nil {
		s.next.prev = s.prev
	} else {
		s.file.last = s.prev
	}
	atomic.AddInt64(&readBufferUsed, -int64(cap(s.page.Data)))
	s.page.Release()
}

type session struct {
	lastOffset uint64    //最后一次读取到位置(off+len)
	total      uint64    //当前session读取的数据总量
	readahead  uint64    //预读窗口
	atime      time.Time //access time
}

type fileReader struct {
	// protected by itself
	inode    Ino                   //文件inode
	length   uint64                //文件长度
	err      syscall.Errno         //文件IO中发生错误的错误码
	tried    uint32                //失败重试次数
	sessions [readSessions]session //固定长度为2的数组，初始化时即默认赋值为初始值
	slices   *sliceReader          //读slice链表头
	last     **sliceReader         //读slice链表尾

	sync.Mutex
	closing bool //文件关闭

	// protected by r
	refs uint16      //引用计数
	next *fileReader //fileReader链表
	r    *dataReader //实际读取数据的实现
}

func (f *fileReader) GetLength() uint64 {
	f.Lock()
	defer f.Unlock()
	return f.length
}

// protected by f
// 新建一个读Slice，并在后台读取数据
func (f *fileReader) newSlice(block *frange) *sliceReader {
	s := &sliceReader{}
	s.file = f
	s.lastAccess = time.Now()
	s.indx = uint32(block.off / meta.ChunkSize)
	s.block = &frange{block.off, block.len} // random read
	blockend := (block.off/f.r.blockSize + 1) * f.r.blockSize
	if s.block.end() > f.length {
		s.block.len = f.length - s.block.off
	}
	//每个Slice最多只包含一个block大小的数据
	if s.block.end() > blockend {
		s.block.len = blockend - s.block.off
	}
	//返回新的block用于生成新的Slice
	block.off = s.block.end()
	block.len -= s.block.len
	//添加进双向链表，直接添加到最后，并没有进行排序
	s.page = chunk.NewOffPage(int(s.block.len))
	s.cond = utils.NewCond(&f.Mutex)
	s.prev = f.last
	*(f.last) = s
	f.last = &(s.next)
	go s.run()
	atomic.AddInt64(&readBufferUsed, int64(cap(s.page.Data)))
	//新生成的Slice默认状态为NEW
	return s
}

func (f *fileReader) delete() {
	r := f.r
	i := r.files[f.inode]
	if i == f {
		if i.next != nil {
			r.files[f.inode] = i.next
		} else {
			delete(r.files, f.inode)
		}
	} else {
		for i != nil {
			if i.next == f {
				i.next = f.next
				break
			}
			i = i.next
		}
	}
	f.next = nil
}

func (f *fileReader) acquire() {
	f.r.Lock()
	defer f.r.Unlock()
	f.refs++
}

func (f *fileReader) release() {
	f.r.Lock()
	defer f.r.Unlock()
	f.refs--
	if f.refs == 0 && f.slices == nil {
		f.delete()
	}
}

// 根据预读策略按照要读数据的范围选择一个session
func (f *fileReader) guessSession(block *frange) int {
	idx := -1
	var closestOff uint64
	for i, ses := range f.sessions {
		//block.off在当前session要预读的数据+一个block之间
		if ses.lastOffset > closestOff && ses.lastOffset <= block.off && block.off <= ses.lastOffset+ses.readahead+f.r.blockSize {
			idx = i
			closestOff = ses.lastOffset
		}
	}
	//第一次轮询未找到
	if idx == -1 {
		//尝试选择那些 block.off 落在 session 的“预读回退区间”内的 session。这里通过 bt（readahead/8，最小为 blockSize）计算回退区间的起点 min，优先选择 lastOffset 最小的 session。
		for i, ses := range f.sessions {
			bt := ses.readahead / 8
			if bt < f.r.blockSize {
				bt = f.r.blockSize
			}
			min := ses.lastOffset - bt
			if ses.lastOffset < bt {
				min = 0
			}
			if min <= block.off && block.off < ses.lastOffset && (closestOff == 0 || ses.lastOffset < closestOff) {
				idx = i
				closestOff = ses.lastOffset
			}
		}
	}
	//第二次轮询也未找到
	if idx == -1 {
		for i, ses := range f.sessions {
			//优先选择未使用的session
			if ses.total == 0 {
				idx = i
				break
			}
			//选择最近使用的session
			if idx == -1 || ses.atime.Before(f.sessions[idx].atime) {
				idx = i
			}
		}
		//初始化session字段
		f.sessions[idx].lastOffset = block.off
		f.sessions[idx].total = block.len
		f.sessions[idx].readahead = 0
	} else {
		//将当前读取的block长度添加到total中
		if block.end() > f.sessions[idx].lastOffset {
			f.sessions[idx].total += block.end() - f.sessions[idx].lastOffset
		}
	}
	f.sessions[idx].atime = time.Now()
	return idx
}

// 确认session并调整预读窗口大小
func (f *fileReader) checkReadahead(block *frange) int {
	idx := f.guessSession(block)
	ses := &f.sessions[idx]
	seqdata := ses.total
	readahead := ses.readahead
	used := uint64(atomic.LoadInt64(&readBufferUsed))
	//还没有需要预读的数据 && 可预读的数据 >= blocksize && (本次读取是从头开始的 || session中已经预取的数据量超过本次要读取的数据量)
	if readahead == 0 && f.r.blockSize <= f.r.readAheadMax && (block.off == 0 || seqdata > block.len) { // begin with read-ahead turned on
		ses.readahead = f.r.blockSize //预读一个block
	} else if readahead < f.r.readAheadMax && seqdata >= readahead && f.r.readAheadTotal-used > readahead*4 { //要预读的数据量 < 单次预读的最大数据量 && 已经预读成功的数据 >= 要预读的数据 && 剩余可用内存 > 4*要预读数据量
		ses.readahead *= 2 //预读数据量扩容到2倍。多预读一些内容
	} else if readahead >= f.r.blockSize && (f.r.readAheadTotal-used < readahead/2 || seqdata < readahead/4) { //要预读的数据量 >= blocksize && (可用于预读的内存容量 < 要预读数据的一半 || 已经预读成功的数据量 < 要预读数据量的1/4 )
		ses.readahead /= 2 //预读数据量减半
	}

	//要预读的数据量还是 >= blocksize,预读
	if ses.readahead >= f.r.blockSize {
		ahead := frange{block.end(), ses.readahead}
		f.readAhead(&ahead)
	}

	//无论是否预读，移动session中上一次访问位置
	if block.end() > ses.lastOffset {
		ses.lastOffset = block.end()
	}
	return idx
}

// 判断block是否在预读回退范围内。若是则需要保留，否则不需要
func (f *fileReader) need(block *frange) bool {
	for _, ses := range f.sessions {
		if ses.total == 0 {
			break
		}
		bt := ses.readahead / 8
		if bt < f.r.blockSize {
			bt = f.r.blockSize
		}
		b := &frange{ses.lastOffset - bt, ses.readahead*2 + f.r.blockSize*2}
		if ses.lastOffset < bt {
			b.off = 0
		}
		//Slice中的数据与预读范围有重叠
		if block.overlap(b) {
			return true
		}
	}
	return false
}

// cleanup unused requests
// block是本次读取请求的off+length
// 合并读请求，同时清理不再被需要的Slice
func (f *fileReader) cleanupRequests(block *frange) {
	now := time.Now()
	var cnt int
	f.visit(func(s *sliceReader) bool {
		/*
			删除读Slice缓存的条件，满足以下任意一条：
				1. 当前Slice的状态已经不合法
				2. 要读取的数据与当前Slice没有交集 && 当前Slice已经超过30秒
				3. 当前Slice与调整后的预读范围完全不重叠，认为其已经不再被需要
		*/
		if !s.state.valid() /*状态不合法*/ ||
			!block.overlap(s.block) /*要读取的数据与s.block不重叠*/ &&
				(s.lastAccess.Add(time.Second*30).Before(now) /*最后访问时间已经超过30秒*/ ||
					!f.need(s.block) /*s.block不再被需要*/) {
			s.drop()
		} else if !block.overlap(s.block) { /*要读取的数据与当前Slice没有交集*/
			cnt++
		}
		return true
	})
	//调整读取次数
	f.visit(func(s *sliceReader) bool {
		if !block.overlap(s.block) /*要读取的数据与当前Slice没有交集*/ && cnt > f.r.maxRequests /*要读取个数超过最大请求数*/ {
			s.drop()
			cnt--
		}
		return cnt > f.r.maxRequests
	})
}

func (f *fileReader) releaseIdleBuffer() {
	f.Lock()
	defer f.Unlock()
	now := time.Now()
	var idle = time.Minute
	used := atomic.LoadInt64(&readBufferUsed)
	if used > int64(f.r.readAheadTotal) {
		idle /= time.Duration(used / int64(f.r.readAheadTotal))
	}
	f.visit(func(s *sliceReader) bool {
		if !s.state.valid() || s.lastAccess.Add(idle).Before(now) || !f.need(s.block) {
			s.drop()
		}
		return true
	})
}

// 将当前请求与所有Slice进行对比，找到本次请求波及到的所有Slice，返回这些Slice的off+end
func (f *fileReader) splitRange(block *frange) []uint64 {
	ranges := []uint64{block.off, block.end()}
	contain := func(p uint64) bool {
		for _, i := range ranges {
			if i == p {
				return true
			}
		}
		return false
	}
	f.visit(func(s *sliceReader) bool {
		if s.state.valid() {
			if block.contain(s.block.off) && !contain(s.block.off) {
				ranges = append(ranges, s.block.off)
			}
			if block.contain(s.block.end()) && !contain(s.block.end()) {
				ranges = append(ranges, s.block.end())
			}
		}
		return true
	})
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i] < ranges[j]
	})
	return ranges
}

// protected by f
// 预读
func (f *fileReader) readAhead(block *frange) {
	f.visit(func(r *sliceReader) bool {
		//读取请求的offset落在Slice数据范围内
		if r.state.valid() && r.block.off <= block.off && r.block.end() > block.off {
			//读取长度大于一个block && 读取偏移地址==Slice偏移地址 && Slice偏移地址是block对齐的
			if r.state == READY && block.len > f.r.blockSize && r.block.off == block.off && r.block.off%f.r.blockSize == 0 {
				// next block is ready, reduce readahead by a block
				//预读半个block
				block.len -= f.r.blockSize / 2
			}
			//读取的数据长度>=Slice长度
			if r.block.end() <= block.end() {
				//需要再读取Slice中没有的数据
				block.len = block.end() - r.block.end()
			} else {
				//数据已全部在Slice中
				block.len = 0
			}
			//请求从Slice的end处开始
			block.off = r.block.end()
		}
		return true
	})
	if block.len > 0 && block.off < f.length && uint64(atomic.LoadInt64(&readBufferUsed)) < f.r.readAheadTotal {
		if block.len < f.r.blockSize {
			//增加读取数据量，block对齐
			block.len += f.r.blockSize - block.end()%f.r.blockSize // align to end of a block
		}
		//新建Slice并在后台读取数据
		f.newSlice(block)
		//循环读取，直至完成
		if block.len > 0 {
			f.readAhead(block)
		}
	}
}

type req struct {
	frange
	s *sliceReader
}

// 根据ranges准备请求，如果数据不重叠，会生成新的Slice；若重叠则直接从已有的Slice中返回
func (f *fileReader) prepareRequests(ranges []uint64) []*req {
	var reqs []*req
	edges := len(ranges)
	for i := 0; i < edges-1; i++ {
		var added bool
		b := frange{ranges[i], ranges[i+1] - ranges[i]}
		f.visit(func(s *sliceReader) bool {
			if !added && s.state.valid() && s.block.include(&b) {
				s.refs++
				s.lastAccess = time.Now()
				reqs = append(reqs, &req{frange{ranges[i] - s.block.off, b.len}, s})
				added = true
				return false
			}
			return true
		})
		if !added {
			for b.len > 0 {
				s := f.newSlice(&b)
				s.refs++
				reqs = append(reqs, &req{frange{0, s.block.len}, s})
			}
		}
	}
	return reqs
}

func (f *fileReader) shouldStop() bool {
	return f.err != 0 || f.closing
}

// 等待异步读取IO返回并返回读到的数据
func (f *fileReader) waitForIO(ctx meta.Context, reqs []*req, buf []byte) (int, syscall.Errno) {
	start := time.Now()
	//等待每一个请求完成
	for _, req := range reqs {
		s := req.s
		for s.state != READY && uint64(s.currentPos) < s.block.len {
			if s.cond.WaitWithTimeout(time.Second) {
				if ctx.Canceled() {
					logger.Warnf("read %d interrupted after %s", f.inode, time.Since(start))
					return 0, syscall.EINTR
				}
			}
			if f.shouldStop() {
				return 0, f.err
			}
		}
	}

	var n int
	for _, req := range reqs {
		s := req.s
		if req.off < s.block.len && s.block.off+req.off < f.length {
			if req.end() > s.block.len {
				logger.Warnf("not enough bytes (%d < %d), restart read", s.block.len, req.end())
				return 0, syscall.EAGAIN
			}
			if s.block.off+req.end() > f.length {
				req.len = f.length - s.block.off - req.off
			}
			n += copy(buf[n:], s.page.Data[req.off:req.end()])
		}
	}
	return n, 0
}

func (f *fileReader) Read(ctx meta.Context, offset uint64, buf []byte) (int, syscall.Errno) {
	if f.r.readBufferUsed() > f.r.bufferSize {
		time.Sleep(time.Millisecond * 10)             // slow down
		for f.r.readBufferUsed() > f.r.bufferSize*2 { // readahead uses 80% of buffer, stop here to avoid OOM
			time.Sleep(time.Millisecond * 100)
		}
	}
	f.Lock()
	defer f.Unlock()
	f.acquire()
	defer f.release()

	if f.err != 0 || f.closing {
		return 0, f.err
	}

	size := uint64(len(buf))
	if offset >= f.length || size == 0 {
		return 0, 0
	}
	block := &frange{offset, size}
	if block.end() > f.length {
		block.len = f.length - block.off
	}

	//清理fileReader中已经无用的Slice
	f.cleanupRequests(block)
	var lastBS uint64 = 32 << 10 //尝试预读32K？
	//如果预读32K之后超过了文件长度
	if block.off+lastBS > f.length {
		//预读到文件尾部
		lastblock := frange{f.length - lastBS, lastBS}
		if f.length < lastBS { //文件长度小于32K，则直接将整个文件进行预读
			lastblock = frange{0, f.length}
		}
		//预读
		f.readAhead(&lastblock)
	}
	//将当前请求与所有Slice进行对比，找到本次请求波及到的所有Slice，返回这些Slice的off+end
	ranges := f.splitRange(block)
	//根据ranges准备请求，如果数据不重叠，会生成新的Slice
	reqs := f.prepareRequests(ranges)
	//请求结束后，释放不再被使用的内存
	defer func() {
		for _, req := range reqs {
			s := req.s
			s.refs--
			if s.refs == 0 && s.state == INVALID {
				s.delete()
			}
		}
	}()
	//确认session并调整预读窗口大小
	f.checkReadahead(block)
	//等待所有读取动作返回
	return f.waitForIO(ctx, reqs, buf)
}

// 从头到尾遍历fileReader中的所有Slice
func (f *fileReader) visit(fn func(s *sliceReader) bool) {
	var next *sliceReader
	for s := f.slices; s != nil; s = next {
		next = s.next
		if !fn(s) {
			break
		}
	}
}

func (f *fileReader) Close(ctx meta.Context) {
	f.Lock()
	f.closing = true
	f.visit(func(s *sliceReader) bool {
		s.drop()
		return true
	})
	f.release()
	f.Unlock()
}

type dataReader struct {
	sync.Mutex
	m              meta.Meta
	store          chunk.ChunkStore    //cachedStore
	files          map[Ino]*fileReader //记录所有文件的读缓存。每个文件的读缓存包含多个filereader以链表形式组织，每个filereader中的Slice以链表形式组织
	blockSize      uint64              //默认4M
	bufferSize     int64               //total read/write buffering in MiB,默认300M
	readAheadMax   uint64              //每次readAhead的最大值
	readAheadTotal uint64              //整个系统中readAhead大小
	maxRequests    int                 //每次readAhead可以下发的最大请求数
	maxRetries     uint32              //最大重试次数
}

func NewDataReader(conf *Config, m meta.Meta, store chunk.ChunkStore) DataReader {
	var readAheadTotal = 256 << 20 //256M
	if conf.Chunk.BufferSize > 0 {
		readAheadTotal = int(conf.Chunk.BufferSize / 10 * 8) // 80% of total buffer
	}
	readAheadMax := min(conf.Chunk.Readahead, readAheadTotal)
	r := &dataReader{
		m:              m,
		store:          store,
		files:          make(map[Ino]*fileReader),
		blockSize:      uint64(conf.Chunk.BlockSize),
		bufferSize:     int64(conf.Chunk.BufferSize),
		readAheadTotal: uint64(readAheadTotal),
		readAheadMax:   uint64(readAheadMax),
		maxRequests:    readAheadMax/conf.Chunk.BlockSize*readSessions + 1,
		maxRetries:     uint32(conf.Meta.Retries),
	}
	go r.checkReadBuffer()
	return r
}

func (r *dataReader) readBufferUsed() int64 {
	used := atomic.LoadInt64(&readBufferUsed)
	return used
}

func (r *dataReader) checkReadBuffer() {
	for {
		r.Lock()
		for _, f := range r.files {
			for f != nil {
				r.Unlock()
				f.releaseIdleBuffer()
				r.Lock()
				f = f.next
			}
		}
		r.Unlock()
		time.Sleep(time.Second)
	}
}

func (r *dataReader) Open(inode Ino, length uint64) FileReader {
	f := &fileReader{
		r:      r,
		inode:  inode,
		length: length,
	}
	f.last = &(f.slices)

	r.Lock()
	f.refs = 1
	f.next = r.files[inode]
	r.files[inode] = f
	r.Unlock()
	return f
}

func (r *dataReader) visit(inode Ino, fn func(*fileReader)) {
	// r could be hold inside f, so Unlock r first to avoid deadlock
	r.Lock()
	var fs []*fileReader
	f := r.files[inode]
	for f != nil {
		fs = append(fs, f)
		f = f.next
	}
	r.Unlock()
	for _, f := range fs {
		f.Lock()
		fn(f)
		f.Unlock()
	}
}

func (r *dataReader) Truncate(inode Ino, length uint64) {
	r.visit(inode, func(f *fileReader) {
		if length < f.length {
			f.visit(func(s *sliceReader) bool {
				if s.block.off+s.block.len > length {
					s.invalidate()
				}
				return true
			})
		}
		f.length = length
	})
}

func (r *dataReader) Invalidate(inode Ino, off, length uint64) {
	b := frange{off, length}
	r.visit(inode, func(f *fileReader) {
		if off+length > f.length {
			f.length = off + length
		}
		f.visit(func(s *sliceReader) bool {
			if b.overlap(s.block) {
				s.invalidate()
			}
			return true
		})
	})
}

// 将数据读入page对应的数据中
func (r *dataReader) readSlice(ctx context.Context, s *meta.Slice, page *chunk.Page, off int) error {
	buf := page.Data
	read := 0
	if s.Id == 0 {
		for read < len(buf) {
			buf[read] = 0
			read++
		}
		return nil
	}

	reader := r.store.NewReader(s.Id, int(s.Size))
	for read < len(buf) {
		p := page.Slice(read, len(buf)-read)
		n, err := reader.ReadAt(ctx, p, off+int(s.Off))
		p.Release()
		if n == 0 && err != nil {
			logger.Warningf("fail to read sliceId %d (off:%d, size:%d, clen: %d, inode: %d): %s",
				s.Id, off+int(s.Off), len(buf)-read, s.Size, ctx.Value(meta.CtxKey("inode")), err)
			return err
		}
		read += n
		off += n
	}
	return nil
}

// 读取对应数据到page中。offset是在chunk中的偏移，要读取多少长度由page决定
func (r *dataReader) Read(ctx context.Context, page *chunk.Page, slices []meta.Slice, offset uint32) int {
	if len(slices) > 16 {
		return r.readManySlices(ctx, page, slices, offset)
	}
	read := 0
	var pos uint32
	errs := make(chan error, 10)
	waits := 0
	buf := page.Data
	size := len(buf)
	//先找到要读取数据offset所在的Slice，然后按照Slice顺序读取数据，直到读到要读取数据的end
	//每次读取一整个Slice
	for i := 0; i < len(slices); i++ {
		if read < size && offset < pos+slices[i].Len {
			toread := min(size-read, int(pos+slices[i].Len-offset))
			//异步读取数据
			go func(s *meta.Slice, p *chunk.Page, off, pos uint32) {
				defer p.Release()
				errs <- r.readSlice(ctx, s, p, int(off))
			}(&slices[i], page.Slice(read, toread), offset-pos, pos)
			read += toread
			offset += uint32(toread)
			waits++
		}
		pos += slices[i].Len
	}
	for read < size {
		buf[read] = 0
		read++
	}
	var err error
	// wait for all goroutine to return, otherwise they may access invalid memory
	for waits > 0 {
		if e := <-errs; e != nil {
			err = e
		}
		waits--
	}
	if err != nil {
		return 0
	}
	return read
}

// 读取超过16个Slice时
func (r *dataReader) readManySlices(ctx context.Context, page *chunk.Page, slices []meta.Slice, offset uint32) int {
	read := 0
	var pos uint32
	var err error
	errs := make(chan error, 10)
	waits := 0
	buf := page.Data
	size := len(buf)
	concurrency := make(chan byte, 16)

SLICES:
	for i := 0; i < len(slices); i++ {
		if read < size && offset < pos+slices[i].Len {
			toread := min(size-read, int(pos+slices[i].Len-offset))
		WAIT:
			for {
				select {
				case concurrency <- 1:
					break WAIT
				case e := <-errs:
					waits--
					if e != nil {
						err = e
						break SLICES
					}
				}
			}
			go func(s *meta.Slice, p *chunk.Page, off int, pos uint32) {
				defer p.Release()
				errs <- r.readSlice(ctx, s, p, off)
				<-concurrency
			}(&slices[i], page.Slice(read, toread), int(offset-pos), pos)

			read += toread
			offset += uint32(toread)
			waits++
		}
		pos += slices[i].Len
	}
	// wait for all jobs done, otherwise they may access invalid memory
	for waits > 0 {
		if e := <-errs; e != nil {
			err = e
		}
		waits--
	}
	if err != nil {
		return 0
	}
	for read < size {
		buf[read] = 0
		read++
	}
	return read
}
