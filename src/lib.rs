#![allow(dead_code)]
#![allow(non_snake_case, non_camel_case_types)]
include!(concat!(env!("OUT_DIR"), "/ublk_cmd.rs"));

use anyhow::Result as AnyRes;
use bitmaps::Bitmap;
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use log::{error, info, trace};
use std::cell::RefCell;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::{env, fs};

const UBLK_IO_RES_ABORT: i32 = -libc::ENODEV;

#[inline(always)]
fn round_up(val: u32, rnd: u32) -> u32 {
    (val + rnd - 1) & !(rnd - 1)
}

#[derive(Debug, Default, Copy, Clone)]
pub struct UblkQueueAffinity {
    affinity: Bitmap<1024>,
}

impl UblkQueueAffinity {
    pub fn new() -> UblkQueueAffinity {
        UblkQueueAffinity {
            affinity: Bitmap::new(),
        }
    }

    pub fn buf_len(&self) -> usize {
        1024 / 8
    }

    pub fn addr(&self) -> *const u8 {
        self.affinity.as_bytes().as_ptr()
    }
    pub fn to_bits_vec(&self) -> Vec<usize> {
        self.affinity.into_iter().collect()
    }
}

union CtrlCmd {
    ctrl_cmd: ublksrv_ctrl_cmd,
    buf: [u8; 80],
}

const CTRL_CMD_HAS_DATA: u32 = 1;
const CTRL_CMD_HAS_BUF: u32 = 2;

#[derive(Debug, Default, Copy, Clone)]
struct UblkCtrlCmdData {
    cmd_op: u32,
    flags: u32,
    data: [u64; 2],
    addr: u64,
    len: u32,
}

fn ublk_ctrl_prep_cmd(fd: i32, dev_id: u32, data: &UblkCtrlCmdData) -> squeue::Entry128 {
    let cmd = ublksrv_ctrl_cmd {
        addr: if (data.flags & CTRL_CMD_HAS_BUF) != 0 {
            data.addr
        } else {
            0
        },
        len: if (data.flags & CTRL_CMD_HAS_BUF) != 0 {
            data.len as u16
        } else {
            0
        },
        data: if (data.flags & CTRL_CMD_HAS_DATA) != 0 {
            [data.data[0] as u64]
        } else {
            [0]
        },
        dev_id: dev_id,
        queue_id: u16::MAX,
        ..Default::default()
    };
    let c_cmd = CtrlCmd { ctrl_cmd: cmd };

    opcode::UringCmd80::new(types::Fd(fd), data.cmd_op)
        .cmd(unsafe { c_cmd.buf })
        .build()
}

fn ublk_ctrl_cmd(ctrl: &mut UblkCtrl, data: &UblkCtrlCmdData) -> AnyRes<i32> {
    let sqe = ublk_ctrl_prep_cmd(ctrl.file.as_raw_fd(), ctrl.dev_info.dev_id, data);

    unsafe {
        ctrl.ring.submission().push(&sqe).expect("submission fail");
    }
    ctrl.ring.submit_and_wait(1)?;

    let cqe = ctrl.ring.completion().next().expect("cqueue is empty");
    match cqe.result() {
        0 => Ok(0),
        e => Err(anyhow::anyhow!(e)),
    }
}

/// UBLK controller
///
/// Responsible for:
///
/// 1) adding and removing ublk char device(/dev/ublkcN)
///
/// 2) send all kinds of control commands
///
/// 3) exporting device as json file
pub struct UblkCtrl {
    file: fs::File,
    pub dev_info: ublksrv_ctrl_dev_info,
    pub json: serde_json::Value,
    for_add: bool,
    ring: IoUring<squeue::Entry128>,
}

impl Drop for UblkCtrl {
    fn drop(&mut self) {
        trace!("ctrl: device {} dropped", self.dev_info.dev_id,);
        if self.for_add {
            if let Err(r) = self.del() {
                //Maybe deleted from other utilities, so no warn or error:w
                trace!("Delete char device {} failed {}", self.dev_info.dev_id, r);
            }
        }
    }
}

impl UblkCtrl {
    pub fn new(id: i32, nr_queues: u32, depth: u32, for_add: bool) -> AnyRes<UblkCtrl> {
        let ring = IoUring::<squeue::Entry128, cqueue::Entry>::builder().build(16)?;
        let info = ublksrv_ctrl_dev_info {
            nr_hw_queues: nr_queues as u16,
            queue_depth: depth as u16,
            max_io_buf_bytes: 512_u32 << 10,
            dev_id: id as u32,
            ublksrv_pid: unsafe { libc::getpid() } as i32,
            ..Default::default()
        };
        let fd = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/ublk-control")?;

        let mut dev = UblkCtrl {
            file: fd,
            dev_info: info,
            json: serde_json::json!({}),
            ring: ring,
            for_add: for_add,
        };

        //add cdev if the device is for adding device
        if dev.for_add {
            dev.add()?;
        }
        trace!("ctrl: device {} created", dev.dev_info.dev_id);

        Ok(dev)
    }

    fn add(&mut self) -> AnyRes<i32> {
        let data: UblkCtrlCmdData = UblkCtrlCmdData {
            cmd_op: UBLK_CMD_ADD_DEV,
            flags: CTRL_CMD_HAS_BUF,
            addr: std::ptr::addr_of!(self.dev_info) as u64,
            len: core::mem::size_of::<ublksrv_ctrl_dev_info>() as u32,
            data: [0, 0],
        };

        ublk_ctrl_cmd(self, &data)
    }

    pub fn del(&mut self) -> AnyRes<i32> {
        let data: UblkCtrlCmdData = UblkCtrlCmdData {
            cmd_op: UBLK_CMD_DEL_DEV,
            ..Default::default()
        };

        ublk_ctrl_cmd(self, &data)
    }

    pub fn get_info(&mut self) -> AnyRes<i32> {
        let data: UblkCtrlCmdData = UblkCtrlCmdData {
            cmd_op: UBLK_CMD_GET_DEV_INFO,
            flags: CTRL_CMD_HAS_BUF,
            addr: std::ptr::addr_of!(self.dev_info) as u64,
            len: core::mem::size_of::<ublksrv_ctrl_dev_info>() as u32,
            ..Default::default()
        };

        ublk_ctrl_cmd(self, &data)
    }

    pub fn start(&mut self, pid: i32) -> AnyRes<i32> {
        let data: UblkCtrlCmdData = UblkCtrlCmdData {
            cmd_op: UBLK_CMD_START_DEV,
            flags: CTRL_CMD_HAS_DATA,
            data: [pid as u64, 0],
            ..Default::default()
        };

        ublk_ctrl_cmd(self, &data)
    }

    pub fn stop(&mut self) -> AnyRes<i32> {
        let data: UblkCtrlCmdData = UblkCtrlCmdData {
            cmd_op: UBLK_CMD_STOP_DEV,
            ..Default::default()
        };

        ublk_ctrl_cmd(self, &data)
    }

    pub fn get_params(&mut self, params: &mut ublk_params) -> AnyRes<i32> {
        params.len = core::mem::size_of::<ublk_params>() as u32;
        let data: UblkCtrlCmdData = UblkCtrlCmdData {
            cmd_op: UBLK_CMD_GET_PARAMS,
            flags: CTRL_CMD_HAS_BUF,
            addr: std::ptr::addr_of!(params) as u64,
            len: params.len,
            ..Default::default()
        };

        ublk_ctrl_cmd(self, &data)
    }

    pub fn set_params(&mut self, params: &ublk_params) -> AnyRes<i32> {
        let mut p = params.clone();

        p.len = core::mem::size_of::<ublk_params>() as u32;
        let data: UblkCtrlCmdData = UblkCtrlCmdData {
            cmd_op: UBLK_CMD_SET_PARAMS,
            flags: CTRL_CMD_HAS_BUF,
            addr: std::ptr::addr_of!(p) as u64,
            len: p.len,
            ..Default::default()
        };

        ublk_ctrl_cmd(self, &data)
    }

    pub fn get_queue_affinity(&mut self, q: u32, bm: &mut UblkQueueAffinity) -> AnyRes<i32> {
        let data: UblkCtrlCmdData = UblkCtrlCmdData {
            cmd_op: UBLK_CMD_GET_QUEUE_AFFINITY,
            flags: CTRL_CMD_HAS_BUF | CTRL_CMD_HAS_DATA,
            addr: bm.addr() as u64,
            data: [q as u64, 0],
            len: bm.buf_len() as u32,
            ..Default::default()
        };
        ublk_ctrl_cmd(self, &data)
    }

    pub fn flush_json(&mut self) -> AnyRes<i32> {
        let run_path = ublk_run_path(self.dev_info.dev_id);
        let mut run_file = fs::File::create(&run_path)?;

        run_file.write_all(self.json.to_string().as_bytes())?;
        Ok(0)
    }

    pub fn build_json(&mut self, dev: &UblkDev, qdata: Vec<(UblkQueueAffinity, i32)>) {
        let tgt_data = self.json["target_data"].clone();
        let mut map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();

        for (qid, (data, tid)) in qdata.iter().enumerate() {
            map.insert(
                format!("{}", qid),
                serde_json::json!({
                    "qid": qid,
                    "tid": tid,
                    "affinity": data.to_bits_vec(),
                }),
            );
        }

        let mut json = serde_json::json!({
                    "dev_info": dev.dev_info,
                    "target": dev.tgt,
        });

        json["target_data"] = tgt_data;
        json["queues"] = serde_json::Value::Object(map);

        self.json = json;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UblkTgt {
    pub tgt_type: String,
    pub dev_size: u64,
    //const struct ublk_tgt_ops *ops;
    pub params: ublk_params,
}

pub struct UblkTgtData {
    pub fds: [i32; 32],
    pub nr_fds: i32,
    pub data: Option<*const u8>,
}

pub struct UblkDev {
    pub dev_info: ublksrv_ctrl_dev_info,
    //q: Vec<ublk_queue>,
    ops: Box<dyn UblkTgtOps>,

    //fds[0] points to /dev/ublkcN
    cdev_file: fs::File,

    pub tgt: RefCell<UblkTgt>,
    pub tdata: RefCell<UblkTgtData>,
}

#[inline(always)]
fn ublk_run_path(dev_id: u32) -> String {
    format!("{}/ublk/{:04}.json", std::env::temp_dir().display(), dev_id)
}

unsafe impl Send for UblkDev {}
unsafe impl Sync for UblkDev {}

impl UblkDev {
    pub fn new(
        ops: Box<dyn UblkTgtOps>,
        ctrl: &mut UblkCtrl,
        tgt_type: &String,
    ) -> AnyRes<UblkDev> {
        let tgt = UblkTgt {
            tgt_type: tgt_type.to_string(),
            ..Default::default()
        };
        let mut data = UblkTgtData {
            fds: [0_i32; 32],
            nr_fds: 0,
            data: None,
        };

        let info = ctrl.dev_info.clone();
        let cdev_path = format!("/dev/ublkc{}", info.dev_id);
        let cdev_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(cdev_path)?;

        let run_path = ublk_run_path(info.dev_id);
        if let Some(parent_dir) = std::path::Path::new(&run_path).parent() {
            fs::create_dir_all(parent_dir)?;
        }

        data.fds[0] = cdev_file.as_raw_fd();
        data.nr_fds = 1;

        let dev = UblkDev {
            ops: ops,
            dev_info: info,
            cdev_file: cdev_file,
            tgt: RefCell::new(tgt),
            tdata: RefCell::new(data),
        };

        ctrl.json = dev.ops.init_tgt(&dev)?;
        info!("dev {} initialized", dev.dev_info.dev_id);

        Ok(dev)
    }

    //private method for drop
    fn deinit_cdev(&mut self) -> AnyRes<i32> {
        let id = self.dev_info.dev_id;

        self.ops.deinit_tgt(self);
        fs::remove_file(ublk_run_path(id))?;

        info!("dev {} deinitialized", id);
        Ok(0)
    }
}

impl Drop for UblkDev {
    fn drop(&mut self) {
        if let Err(err) = self.deinit_cdev() {
            error!("deinit cdev {} failed {}", self.dev_info.dev_id, err);
        }
    }
}

pub trait UblkQueueOps {
    fn queue_io(&self, q: &UblkQueue, io: &mut UblkIO, tag: u32) -> AnyRes<i32>;
    fn tgt_io_done(&self, q: &UblkQueue, tag: u32, res: i32, user_data: u64);
}

pub trait UblkTgtOps {
    fn init_tgt(&self, dev: &UblkDev) -> AnyRes<serde_json::Value>;
    fn deinit_tgt(&self, dev: &UblkDev);
}

#[repr(C, align(512))]
struct ublk_dio_buf([u8; 512]);

union IOCmd {
    cmd: ublksrv_io_cmd,
    buf: [u8; 16],
}

#[inline(always)]
#[allow(arithmetic_overflow)]
fn build_user_data(tag: u16, op: u32, tgt_data: u32, is_target_io: bool) -> u64 {
    assert!((op >> 8) == 0 && (tgt_data >> 16) == 0);

    match is_target_io {
        true => (tag as u32 | (op << 16) | (tgt_data << 24) | (1 << 63)) as u64,
        false => (tag as u32 | (op << 16) | (tgt_data << 24)) as u64,
    }
}

#[inline(always)]
fn is_target_io(user_data: u64) -> bool {
    (user_data & (1_u64 << 63)) != 0
}

#[inline(always)]
fn user_data_to_tag(user_data: u64) -> u32 {
    (user_data & 0xffff) as u32
}

#[inline(always)]
fn user_data_to_op(user_data: u64) -> u32 {
    ((user_data >> 16) & 0xff) as u32
}

const UBLK_IO_NEED_FETCH_RQ: u32 = 1_u32 << 0;
const UBLK_IO_NEED_COMMIT_RQ_COMP: u32 = 1_u32 << 1;
const UBLK_IO_FREE: u32 = 1u32 << 2;
pub struct UblkIO {
    buf_addr: *mut u8,
    flags: u32,
    result: i32,
}

impl UblkIO {
    fn is_done(&self) -> bool {
        self.flags & (UBLK_IO_NEED_COMMIT_RQ_COMP | UBLK_IO_FREE) != 0
    }
}

const UBLK_QUEUE_STOPPING: u32 = 1_u32 << 0;
const UBLK_QUEUE_IDLE: u32 = 1_u32 << 1;

/// UBLK queue abstraction
///
/// Responsible for handling ublk IO from ublk driver.
///
/// So far, each queue is handled by one single io_uring.
///
pub struct UblkQueue<'a> {
    pub q_id: u16,
    pub q_depth: u32,
    io_cmd_buf: u64,
    ops: Box<dyn UblkQueueOps>,
    pub dev: &'a UblkDev,
    cmd_inflight: RefCell<u32>,
    q_state: RefCell<u32>,
    ios: RefCell<Vec<UblkIO>>,
    pub q_ring: RefCell<IoUring<squeue::Entry>>,
}

impl Drop for UblkQueue<'_> {
    fn drop(&mut self) {
        let ring = self.q_ring.borrow_mut();
        let dev = self.dev;
        trace!("dev {} queue {} dropped", dev.dev_info.dev_id, self.q_id);

        if let Err(r) = ring.submitter().unregister_files() {
            error!("unregister fixed files failed {}", r);
        }

        let depth = dev.dev_info.queue_depth as u32;
        let cmd_buf_sz = UblkQueue::cmd_buf_sz(depth) as usize;

        //unmap, otherwise our cdev won't be released
        unsafe {
            libc::munmap(self.io_cmd_buf as *mut libc::c_void, cmd_buf_sz);
        }
    }
}

impl UblkQueue<'_> {
    #[inline(always)]
    fn cmd_buf_sz(depth: u32) -> u32 {
        let size = depth * core::mem::size_of::<ublksrv_io_desc>() as u32;
        let page_sz = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u32;

        return round_up(size, page_sz);
    }

    pub fn new(
        ops: Box<dyn UblkQueueOps>,
        q_id: u16,
        dev: &UblkDev,
        sq_depth: u32,
        cq_depth: u32,
        _ring_flags: u64,
    ) -> AnyRes<UblkQueue> {
        let td = dev.tdata.borrow();
        let ring = IoUring::<squeue::Entry, cqueue::Entry>::builder()
            .setup_cqsize(cq_depth)
            .setup_coop_taskrun()
            .build(sq_depth)
            .unwrap();
        let depth = dev.dev_info.queue_depth as u32;
        let cdev_fd = dev.cdev_file.as_raw_fd();
        let cmd_buf_sz = UblkQueue::cmd_buf_sz(depth) as usize;

        ring.submitter()
            .register_files(&td.fds[0..td.nr_fds as usize])
            .unwrap();

        let off = UBLKSRV_CMD_BUF_OFFSET as i64
            + q_id as i64
                * ((UBLK_MAX_QUEUE_DEPTH as usize * core::mem::size_of::<ublksrv_io_desc>())
                    as i64);
        let io_cmd_buf = unsafe {
            libc::mmap(
                0 as *mut libc::c_void,
                cmd_buf_sz,
                libc::PROT_READ,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                cdev_fd,
                off as i64,
            )
        };
        if io_cmd_buf == libc::MAP_FAILED {
            return Err(anyhow::anyhow!("io command buf mapping failed"));
        }

        let mut ios = Vec::<UblkIO>::with_capacity(depth as usize);
        unsafe {
            ios.set_len(depth as usize);
        }
        for io in &mut ios {
            io.buf_addr =
                Vec::<ublk_dio_buf>::with_capacity(dev.dev_info.max_io_buf_bytes as usize / 512)
                    .as_mut_ptr() as *mut u8;
            io.flags = UBLK_IO_NEED_FETCH_RQ | UBLK_IO_FREE;
            io.result = -1;
        }

        let q = UblkQueue {
            ops: ops,
            q_id: q_id,
            q_depth: depth,
            io_cmd_buf: io_cmd_buf as u64,
            dev: dev,
            cmd_inflight: RefCell::new(0),
            q_state: RefCell::new(0),
            q_ring: RefCell::new(ring),
            ios: RefCell::new(ios),
        };

        trace!("dev {} queue {} started", dev.dev_info.dev_id, q_id);

        Ok(q)
    }

    #[inline(always)]
    fn mark_io_done(&self, io: &mut UblkIO, _tag: u16, res: i32) {
        io.flags |= UBLK_IO_NEED_COMMIT_RQ_COMP | UBLK_IO_FREE;
        io.result = res;
    }

    #[inline(always)]
    pub fn get_iod(&self, idx: u32) -> *const ublksrv_io_desc {
        (self.io_cmd_buf + idx as u64 * 24) as *const ublksrv_io_desc
    }

    #[inline(always)]
    #[allow(unused_assignments)]
    fn __queue_io_cmd(&self, ring: &mut IoUring<squeue::Entry>, tag: u16) -> i32 {
        let state = self.q_state.borrow();
        let ios = self.ios.borrow();
        let io = &ios[tag as usize];
        let mut cmd_op = 0_u32;

        if (io.flags & UBLK_IO_FREE) == 0 {
            return 0;
        }

        if (io.flags & UBLK_IO_NEED_COMMIT_RQ_COMP) != 0 {
            cmd_op = UBLK_IO_COMMIT_AND_FETCH_REQ;
        } else if (io.flags & UBLK_IO_NEED_FETCH_RQ) != 0 {
            cmd_op = UBLK_IO_FETCH_REQ;
        } else {
            return 0;
        }

        let cmd = ublksrv_io_cmd {
            tag: tag,
            addr: io.buf_addr as u64,
            q_id: self.q_id,
            result: io.result,
        };
        let io_cmd = IOCmd { cmd: cmd };
        let data = build_user_data(tag, cmd_op, 0, false);

        let sqe = opcode::UringCmd16::new(types::Fixed(0), cmd_op)
            .cmd(unsafe { io_cmd.buf })
            .build()
            .user_data(data);

        unsafe {
            ring.submission().push(&sqe).expect("submission fail");
        }

        trace!(
            "{}: (qid {} tag {} cmd_op {}) iof {} stopping {}",
            "queue_io_cmd",
            self.q_id,
            tag,
            cmd_op,
            io.flags,
            (*state & UBLK_QUEUE_STOPPING) != 0
        );

        1
    }

    #[inline(always)]
    fn queue_io_cmd(&self, ring: &mut IoUring<squeue::Entry>, tag: u16) -> i32 {
        let res = self.__queue_io_cmd(ring, tag);

        if res > 0 {
            let mut cnt = self.cmd_inflight.borrow_mut();
            let mut ios = self.ios.borrow_mut();
            let mut io = &mut ios[tag as usize];

            *cnt += 1;
            io.flags = 0;
        }

        res
    }

    #[inline(always)]
    pub fn submit_fetch_commands(&self) {
        let mut ring = self.q_ring.borrow_mut();
        for i in 0..self.q_depth {
            self.queue_io_cmd(&mut ring, i as u16);
        }
    }

    #[inline(always)]
    fn queue_is_idle(&self) -> bool {
        let cnt = self.cmd_inflight.borrow();
        *cnt == 0
    }

    #[inline(always)]
    fn queue_is_done(&self) -> bool {
        let state = self.q_state.borrow();
        (*state & UBLK_QUEUE_STOPPING) != 0 && self.queue_is_idle()
    }

    #[inline(always)]
    pub fn complete_io(&self, io: &mut UblkIO, tag: u16, res: i32) {
        self.mark_io_done(io, tag, res);
    }

    #[inline(always)]
    fn handle_tgt_cqe(&self, res: i32, data: u64) {
        let tag = user_data_to_tag(data);

        if res < 0 && res != -(libc::EAGAIN) {
            info!(
                "{}: failed tgt io: res {} qid {} tag {}, cmd_op {}\n",
                "handle_tgt_cqe",
                res,
                self.q_id,
                user_data_to_tag(data),
                user_data_to_op(data)
            );

            self.ops.tgt_io_done(self, tag, res, data);
        }
    }

    #[inline(always)]
    #[allow(unused_assignments)]
    fn handle_cqe(&self, e: &cqueue::Entry) {
        let data = e.user_data();
        let res = e.result();
        let tag = user_data_to_tag(data);
        let cmd_op = user_data_to_op(data);
        let mut state = self.q_state.borrow_mut();

        trace!(
            "{}: res {} (qid {} tag {} cmd_op {} target {}) state {}",
            "handle_cqe",
            res,
            self.q_id,
            tag,
            cmd_op,
            is_target_io(data),
            *state
        );

        /* Don't retrieve io in case of target io */
        if is_target_io(data) {
            self.handle_tgt_cqe(res, data);
            return;
        }

        let mut cnt = self.cmd_inflight.borrow_mut();
        let ios = &mut self.ios.borrow_mut();
        let io = &mut ios[tag as usize];

        *cnt -= 1;

        if res == UBLK_IO_RES_ABORT || ((*state & UBLK_QUEUE_STOPPING) != 0) {
            *state |= UBLK_QUEUE_STOPPING;
            io.flags &= !UBLK_IO_NEED_FETCH_RQ;
        }

        if res == UBLK_IO_RES_OK as i32 {
            assert!(tag < self.q_depth);
            self.ops.queue_io(self, io, tag).unwrap();
        } else {
            /*
             * COMMIT_REQ will be completed immediately since no fetching
             * piggyback is required.
             *
             * Marking IO_FREE only, then this io won't be issued since
             * we only issue io with (UBLKSRV_IO_FREE | UBLKSRV_NEED_*)
             *
             * */
            io.flags = UBLK_IO_FREE;
        }
    }

    #[inline(always)]
    fn reap_events_uring(&self, ring: &mut IoUring<squeue::Entry>) -> usize {
        let mut count = 0;
        let mut done = Vec::<u16>::with_capacity(32);
        for cqe in ring.completion() {
            let data = cqe.user_data();
            self.handle_cqe(&cqe);
            count += 1;
            {
                let ios = &mut self.ios.borrow_mut();
                let tag = user_data_to_tag(data);
                let io = &ios[tag as usize];

                if io.is_done() {
                    done.push(tag as u16);
                }
            }
        }

        for tag in done {
            self.queue_io_cmd(ring, tag as u16);
        }

        count
    }

    pub fn process_io(&self) -> i32 {
        {
            let cnt = self.cmd_inflight.borrow();
            let state = self.q_state.borrow();

            info!(
                "dev{}-q{}: to_submit {} inflight cmd {} stopping {}",
                self.dev.dev_info.dev_id,
                self.q_id,
                0,
                *cnt,
                (*state & UBLK_QUEUE_STOPPING)
            );
        }

        {
            let mut ring = self.q_ring.borrow_mut();

            if self.queue_is_done() {
                if ring.submission().is_empty() {
                    return -libc::ENODEV;
                }
            }

            let ret = ring.submit_and_wait(1).unwrap();
            let reapped = self.reap_events_uring(&mut ring);

            {
                let state = self.q_state.borrow();
                info!(
                    "submit result {}, reapped {} stop {} idle {}",
                    ret,
                    reapped,
                    (*state & UBLK_QUEUE_STOPPING),
                    (*state & UBLK_QUEUE_IDLE)
                );
            }
            return reapped as i32;
        }
    }
}
