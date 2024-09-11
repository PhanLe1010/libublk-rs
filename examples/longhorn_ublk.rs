use anyhow::Result;
use bitflags::bitflags;
use clap::{Arg, ArgAction, Command};
// use futures::TryFutureExt;
use ilog::IntLog;
use io_uring::{opcode, squeue, types};
use libublk::helpers::IoBuf;
use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
use libublk::uring_async::ublk_wait_and_handle_ios;
use libublk::{ctrl::UblkCtrl, sys, UblkError, UblkFlags, UblkIORes};
use nix::NixPath;
use serde::Serialize;
use std::borrow::Borrow;
use std::os::fd::AsFd;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;

use async_std::sync::{Arc, Mutex};
// use std::io::prelude::*;
// use std::net::TcpStream;
use std::cell::RefCell;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

use libublk::longhorn_rpc_protocal;
use md5::{Digest, Md5};
use std::fs::File;

#[derive(Debug, Serialize)]
struct LoJson {
    back_file_path: String,
    direct_io: i32,
}

struct LoopTgt {
    back_file_path: String,
    back_file: std::fs::File,
    direct_io: i32,
}

bitflags! {
    #[derive(Default)]
    struct LoFlags: u32 {
        const ASYNC = 0b00000001;
        const FOREGROUND = 0b00000010;
        const ONESHOT = 0b00001000;
    }
}

// Generate ioctl function
const BLK_IOCTL_TYPE: u8 = 0x12; // Defined in linux/fs.h
const BLKGETSIZE64_NR: u8 = 114;
const BLKSSZGET_NR: u8 = 104;
const BLKPBSZGET_NR: u8 = 123;

nix::ioctl_read!(ioctl_blkgetsize64, BLK_IOCTL_TYPE, BLKGETSIZE64_NR, u64);
nix::ioctl_read_bad!(
    ioctl_blksszget,
    nix::request_code_none!(BLK_IOCTL_TYPE, BLKSSZGET_NR),
    i32
);
nix::ioctl_read_bad!(
    ioctl_blkpbszget,
    nix::request_code_none!(BLK_IOCTL_TYPE, BLKPBSZGET_NR),
    u32
);
fn lo_file_size(f: &std::fs::File) -> Result<(u64, u8, u8)> {
    if let Ok(meta) = f.metadata() {
        if meta.file_type().is_block_device() {
            let fd = f.as_raw_fd();
            let mut cap = 0_u64;
            let mut ssz = 0_i32;
            let mut pbsz = 0_u32;

            unsafe {
                let cap_ptr = &mut cap as *mut u64;
                let ssz_ptr = &mut ssz as *mut i32;
                let pbsz_ptr = &mut pbsz as *mut u32;

                ioctl_blkgetsize64(fd, cap_ptr).unwrap();
                ioctl_blksszget(fd, ssz_ptr).unwrap();
                ioctl_blkpbszget(fd, pbsz_ptr).unwrap();
            }

            Ok((cap, ssz.log2() as u8, pbsz.log2() as u8))
        } else if meta.file_type().is_file() {
            Ok((f.metadata().unwrap().len(), 9, 12))
        } else {
            Err(anyhow::anyhow!("unsupported file"))
        }
    } else {
        Err(anyhow::anyhow!("no file meta got"))
    }
}

// setup longhorn target
fn lo_init_tgt(dev: &mut UblkDev, lo: &LoopTgt) -> Result<(), UblkError> {
    log::info!("loop: init_tgt {}", dev.dev_info.dev_id);
    if lo.direct_io != 0 {
        unsafe {
            libc::fcntl(lo.back_file.as_raw_fd(), libc::F_SETFL, libc::O_DIRECT);
        }
    }

    let tgt = &mut dev.tgt;
    let nr_fds = tgt.nr_fds;
    tgt.fds[nr_fds as usize] = lo.back_file.as_raw_fd();
    tgt.nr_fds = nr_fds + 1;

    let sz = { lo_file_size(&lo.back_file).unwrap() };
    tgt.dev_size = sz.0;
    //todo: figure out correct block size
    tgt.params = libublk::sys::ublk_params {
        types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
        basic: libublk::sys::ublk_param_basic {
            logical_bs_shift: sz.1,
            physical_bs_shift: sz.2,
            io_opt_shift: 12,
            io_min_shift: 9,
            max_sectors: dev.dev_info.max_io_buf_bytes >> 9,
            dev_sectors: tgt.dev_size >> 9,
            ..Default::default()
        },
        ..Default::default()
    };
    let val = serde_json::json!({"longhorn_ublk": LoJson { back_file_path: lo.back_file_path.clone(), direct_io: 1 } });
    dev.set_target_json(val);

    Ok(())
}

#[inline]
fn __lo_prep_submit_io_cmd(iod: &libublk::sys::ublksrv_io_desc) -> i32 {
    let op = iod.op_flags & 0xff;

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH
        | libublk::sys::UBLK_IO_OP_READ
        | libublk::sys::UBLK_IO_OP_WRITE => return 0,
        _ => return -libc::EINVAL,
    };
}

#[inline]
fn __lo_make_io_sqe(op: u32, off: u64, bytes: u32, buf_addr: *mut u8) -> io_uring::squeue::Entry {
    match op {
        libublk::sys::UBLK_IO_OP_FLUSH => opcode::SyncFileRange::new(types::Fixed(1), bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        libublk::sys::UBLK_IO_OP_READ => opcode::Read::new(types::Fixed(1), buf_addr, bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        libublk::sys::UBLK_IO_OP_WRITE => opcode::Write::new(types::Fixed(1), buf_addr, bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        _ => panic!(),
    }
}

async fn lo_handle_io_cmd_async(q: &UblkQueue<'_>, tag: u16, buf_addr: *mut u8) -> i32 {
    let iod = q.get_iod(tag);
    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        return res;
    }

    for _ in 0..4 {
        let op = iod.op_flags & 0xff;
        // either start to handle or retry
        let off = (iod.start_sector << 9) as u64;
        let bytes = (iod.nr_sectors << 9) as u32;

        let sqe = __lo_make_io_sqe(op, off, bytes, buf_addr);
        let res = q.ublk_submit_sqe(sqe).await;
        if res != -(libc::EAGAIN) {
            return res;
        }
    }

    return -libc::EAGAIN;
}

fn lo_handle_io_cmd_sync(q: &UblkQueue<'_>, tag: u16, i: &UblkIOCtx, buf_addr: *mut u8) {
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let data = UblkIOCtx::build_user_data(tag as u16, op, 0, true);
    if i.is_tgt_io() {
        let user_data = i.user_data();
        let res = i.result();
        let cqe_tag = UblkIOCtx::user_data_to_tag(user_data);

        assert!(cqe_tag == tag as u32);

        if res != -(libc::EAGAIN) {
            q.complete_io_cmd(tag, buf_addr, Ok(UblkIORes::Result(res)));
            return;
        }
    }

    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        q.complete_io_cmd(tag, buf_addr, Ok(UblkIORes::Result(res)));
    } else {
        let op = iod.op_flags & 0xff;
        // either start to handle or retry
        let off = (iod.start_sector << 9) as u64;
        let bytes = (iod.nr_sectors << 9) as u32;
        let sqe = __lo_make_io_sqe(op, off, bytes, buf_addr).user_data(data);
        q.ublk_submit_sqe_sync(sqe).unwrap();
    }
}

fn q_fn(qid: u16, dev: &UblkDev) {
    let bufs_rc = Rc::new(dev.alloc_queue_io_bufs());
    let bufs = bufs_rc.clone();
    let lo_io_handler = move |q: &UblkQueue, tag: u16, io: &UblkIOCtx| {
        let bufs = bufs_rc.clone();

        lo_handle_io_cmd_sync(q, tag, io, bufs[tag as usize].as_mut_ptr());
    };

    UblkQueue::new(qid, dev)
        .unwrap()
        .regiser_io_bufs(Some(&bufs))
        .submit_fetch_commands(Some(&bufs))
        .wait_and_handle_io(lo_io_handler);
}

const SOCKET_PATH: &str = "/var/run/longhorn-testvol-e.sock";

fn compute_md5_checksum(buf: &[u8]) -> String {
    // Create an Md5 hasher
    let mut hasher = Md5::new();

    // Write the buffer data into the hasher
    hasher.update(buf);

    // Finalize the hash and get the result as bytes
    let result = hasher.finalize();

    // Convert the hash result to a hex string
    format!("{:x}", result)
}

fn q_a_fn(qid: u16, dev: &UblkDev, depth: u16) {
    let q_rc = Rc::new(UblkQueue::new(qid as u16, &dev).unwrap());
    let exe = smol::LocalExecutor::new();
    let mut f_vec = Vec::new();

    // // Establish a TCP connection to the server
    // let mut stream = TcpStream::connect("127.0.0.1:34254").unwrap();
    // // Wrap the stream in Arc<Mutex<TcpStream>> to share it between async tasks
    // let shared_stream = Arc::new(Mutex::new(stream));

    // let stream_arc = Arc::new(Mutex::new(UnixStream::connect(SOCKET_PATH).unwrap()));

    // let stream_org = UnixStream::connect(SOCKET_PATH).unwrap();

    for tag in 0..depth {
        let q = q_rc.clone();
        // let stream_clone = Arc::clone(&shared_stream);
        // let stream_clone = Arc::clone(&stream_arc);

        let mut stream = UnixStream::connect(SOCKET_PATH).unwrap();

        f_vec.push(exe.spawn(async move {
            // create a new message and push it the longhorn_conn.queue
            // wait for the message's response
            // let message = format!("Message from task");
            // let mut stream = stream_clone.lock().await;
            // stream.write_all(message.as_bytes()).await.expect("Failed to send message");
            // stream.flush().await.expect("Failed to flush stream");

            let buf: IoBuf<u8> = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);
            let buf_addr = buf.as_mut_ptr();
            let mut cmd_op = sys::UBLK_U_IO_FETCH_REQ;
            let mut res = 0;
            // let mut stream = stream_clone.borrow_mut();
            // let mut stream_imm = stream_clone.borrow();
            // stream_imm.as_raw_fd();

            // let mut stream = UnixStream::connect(SOCKET_PATH).unwrap();
            // let buf_slice = unsafe { std::slice::from_raw_parts_mut(buf_addr, buf.len()) };
            // let n = stream.read(buf_slice).unwrap();
            let message_header_size = longhorn_rpc_protocal::get_message_header_size();
            let mut request_header: Vec<u8> = vec![0u8; message_header_size];
            let mut response_header: Vec<u8> = vec![0u8; message_header_size];

            q.register_io_buf(tag, &buf);
            loop {
                let cmd_res = q.submit_io_cmd(tag, cmd_op, buf_addr, res).await;
                if cmd_res == sys::UBLK_IO_RES_ABORT {
                    break;
                }

                // lock the longhorn connection
                // prepare the Longhorn message
                // send it to the connection
                // unblock the connection

                // let mut stream = stream_clone.lock().await;

                let iod = q.get_iod(tag);
                let op = iod.op_flags & 0xff;

                let off = (iod.start_sector << 9) as u64;
                let bytes = (iod.nr_sectors << 9) as u32;

                let msgType = match op {
                    libublk::sys::UBLK_IO_OP_READ => {
                        longhorn_rpc_protocal::MessageType::TypeRead as u32
                    }
                    libublk::sys::UBLK_IO_OP_WRITE => {
                        longhorn_rpc_protocal::MessageType::TypeWrite as u32
                    }
                    _ => panic!(),
                };

                let mut req_header = longhorn_rpc_protocal::MessageHeader {
                    MagicVersion: longhorn_rpc_protocal::MAGIC_VERSION,
                    Seq: 1,
                    Type: msgType,
                    Offset: off,
                    Size: bytes,
                    DataLength: 0,
                };

                if (req_header.Type == longhorn_rpc_protocal::MessageType::TypeWrite as u32) {
                    req_header.DataLength = bytes;
                }

                match longhorn_rpc_protocal::write_header(
                    &mut stream,
                    &req_header,
                    &mut request_header,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error: {:?}", e);
                        break;
                    }
                }

                // print!("here 0\n");
                // print!("res_header.Size: {}\n", req_header.Size);
                // print!("res_header.DataLength: {}\n", req_header.DataLength);

                if req_header.DataLength > 0 {
                    let buf_slice = unsafe {
                        std::slice::from_raw_parts_mut(buf_addr, req_header.DataLength as usize)
                    };
                    match stream.write_all(buf_slice) {
                        Ok(_) => {}
                        Err(e) => {
                            println!("Error: {:?}", e);
                            break;
                        }
                    }
                    // println!("Checksum: {}", compute_md5_checksum(buf_slice));
                }

                let mut res_header = longhorn_rpc_protocal::MessageHeader {
                    MagicVersion: 0,
                    Seq: 0,
                    Type: 0,
                    Offset: 0,
                    Size: 0,
                    DataLength: 0,
                };

                match longhorn_rpc_protocal::read_header(
                    &mut stream,
                    &mut res_header,
                    &mut response_header,
                    message_header_size,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error: {:?}", e);
                        break;
                    }
                }

                // print!("here 1\n");
                // print!("res_header.DataLength: {}\n", res_header.DataLength);
                if res_header.DataLength > 0 {
                    let buf_slice = unsafe {
                        std::slice::from_raw_parts_mut(buf_addr, res_header.DataLength as usize)
                    };
                    match stream.read_exact(buf_slice) {
                        Ok(_) => {}
                        Err(e) => {
                            println!("Error: {:?}", e);
                            break;
                        }
                    }
                    // print!("buf_slice.legnth: {}\n", buf_slice.len());
                    // println!(
                    //     "Checksum: {}",
                    //     compute_md5_checksum(&buf_slice[..buf_slice.len()])
                    // );
                }

                // print!("############\n");

                // drop(stream);

                res = get_io_cmd_result(&q, tag);
                cmd_op = sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
            }
        }));
    }

    // stream_rc.borrow_mut().write_all(b"Hello, world!").unwrap();

    // 2nd appoarch:
    // spawn a new OS thread to continuously read from socket and dispatch the res

    ublk_wait_and_handle_ios(&exe, &q_rc);
    smol::block_on(async { futures::future::join_all(f_vec).await });
}

fn get_io_cmd_result(q: &UblkQueue, tag: u16) -> i32 {
    let iod = q.get_iod(tag);
    let bytes = (iod.nr_sectors << 9) as i32;

    bytes
}

fn __loop_add(
    id: i32,
    nr_queues: u32,
    depth: u16,
    buf_sz: u32,
    backing_file: &String,
    ctrl_flags: u64,
    lo_flags: LoFlags,
) {
    let aio = lo_flags.intersects(LoFlags::ASYNC);
    let oneshot = lo_flags.intersects(LoFlags::ONESHOT);
    // LooTgt has to live in the whole device lifetime
    let lo = LoopTgt {
        back_file: std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&backing_file)
            .unwrap(),
        direct_io: 1,
        back_file_path: backing_file.clone(),
    };
    let ctrl = libublk::ctrl::UblkCtrlBuilder::default()
        .name("longhorn_ublk")
        .id(id)
        .ctrl_flags(ctrl_flags)
        .nr_queues(nr_queues.try_into().unwrap())
        .depth(depth)
        .io_buf_bytes(buf_sz)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()
        .unwrap();
    let tgt_init = |dev: &mut UblkDev| lo_init_tgt(dev, &lo);
    let wh = move |d_ctrl: &UblkCtrl| {
        d_ctrl.dump();
        if oneshot {
            d_ctrl.kill_dev().unwrap();
        }
    };

    if aio {
        ctrl.run_target(tgt_init, move |qid, dev: &_| q_a_fn(qid, dev, depth), wh)
            .unwrap();
    } else {
        ctrl.run_target(tgt_init, move |qid, dev: &_| q_fn(qid, dev), wh)
            .unwrap();
    }
}

fn loop_add(
    id: i32,
    nr_queues: u32,
    depth: u16,
    buf_sz: u32,
    backing_file: &String,
    ctrl_flags: u64,
    lo_flags: LoFlags,
) {
    if lo_flags.intersects(LoFlags::FOREGROUND) {
        __loop_add(
            id,
            nr_queues,
            depth,
            buf_sz,
            backing_file,
            ctrl_flags,
            lo_flags,
        );
    } else {
        let daemonize = daemonize::Daemonize::new()
            .stdout(daemonize::Stdio::keep())
            .stderr(daemonize::Stdio::keep());

        match daemonize.start() {
            Ok(_) => __loop_add(
                id,
                nr_queues,
                depth,
                buf_sz,
                backing_file,
                ctrl_flags,
                lo_flags,
            ),
            Err(_) => panic!(),
        }
    }
}

fn main() {
    let matches = Command::new("ublk-loop-example")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("add")
                .about("Add ublk device")
                .arg(
                    Arg::new("number")
                        .short('n')
                        .long("number")
                        .default_value("-1")
                        .allow_hyphen_values(true)
                        .help("device id, -1: auto-allocation")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("queues")
                        .long("queues")
                        .short('q')
                        .default_value("1")
                        .help("nr_hw_queues")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("depth")
                        .long("depth")
                        .short('d')
                        .default_value("64")
                        .help("queue depth: max in-flight io commands")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("buf_size")
                        .long("buf_size")
                        .short('b')
                        .default_value("524288")
                        .help("io buffer size")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("unprivileged")
                        .long("unprivileged")
                        .short('p')
                        .action(ArgAction::SetTrue)
                        .help("enable UBLK_F_UN_PRIVILEGED_DEV"),
                )
                .arg(
                    Arg::new("foreground")
                        .long("foreground")
                        .action(ArgAction::SetTrue)
                        .help("run in foreground mode"),
                )
                .arg(
                    Arg::new("backing_file")
                        .long("backing_file")
                        .short('f')
                        .required(true)
                        .help("backing file")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("async")
                        .long("async")
                        .short('a')
                        .action(ArgAction::SetTrue)
                        .help("use async/await to handle IO command"),
                )
                .arg(
                    Arg::new("oneshot")
                        .long("oneshot")
                        .action(ArgAction::SetTrue)
                        .help("create, dump and remove device automatically"),
                ),
        )
        .subcommand(
            Command::new("del").about("Delete ublk device").arg(
                Arg::new("number")
                    .short('n')
                    .long("number")
                    .required(true)
                    .help("device id")
                    .action(ArgAction::Set),
            ),
        )
        .subcommand(Command::new("list").about("List ublk device"))
        .get_matches();

    match matches.subcommand() {
        Some(("add", add_matches)) => {
            let id = add_matches
                .get_one::<String>("number")
                .unwrap()
                .parse::<i32>()
                .unwrap_or(-1);
            let nr_queues = add_matches
                .get_one::<String>("queues")
                .unwrap()
                .parse::<u32>()
                .unwrap_or(1);
            let depth = add_matches
                .get_one::<String>("depth")
                .unwrap()
                .parse::<u32>()
                .unwrap_or(64);
            let buf_size = add_matches
                .get_one::<String>("buf_size")
                .unwrap()
                .parse::<u32>()
                .unwrap_or(52288);
            let backing_file = add_matches.get_one::<String>("backing_file").unwrap();
            let mut lo_flags: LoFlags = Default::default();

            if add_matches.get_flag("async") {
                lo_flags |= LoFlags::ASYNC;
            };
            if add_matches.get_flag("foreground") {
                lo_flags |= LoFlags::FOREGROUND;
            };
            if add_matches.get_flag("oneshot") {
                lo_flags |= LoFlags::ONESHOT;
            };
            let ctrl_flags: u64 = if add_matches.get_flag("unprivileged") {
                libublk::sys::UBLK_F_UNPRIVILEGED_DEV as u64
            } else {
                0
            };
            loop_add(
                id,
                nr_queues,
                depth.try_into().unwrap(),
                buf_size,
                backing_file,
                ctrl_flags,
                lo_flags,
            );
        }
        Some(("del", add_matches)) => {
            let id = add_matches
                .get_one::<String>("number")
                .unwrap()
                .parse::<i32>()
                .unwrap_or(-1);
            UblkCtrl::new_simple(id).unwrap().del_dev().unwrap();
        }
        Some(("list", _add_matches)) => UblkCtrl::for_each_dev_id(|dev_id| {
            UblkCtrl::new_simple(dev_id as i32).unwrap().dump();
        }),
        _ => {
            println!("unsupported command");
        }
    };
}
