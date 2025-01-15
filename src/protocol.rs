use std::io::{Write, Read};
use bincode;
use crate::{okvs, psi};

use std::sync::mpsc as std_mpsc;
use std::thread;

pub fn setup(num_n: usize, num_m: usize, apart: bool, metric: u32) -> (psi::Receiver, psi::Sender) {
    let psi_rec = psi::Receiver::new(num_n as u64, apart);
    let psi_sed = psi::Sender::new(num_m as u64, psi_rec.publish_pk(), apart, metric);
    return (psi_rec, psi_sed);
}

use std::net::{TcpListener, TcpStream};
use crate::okvs::PointPair;

pub fn run_standard(
    mut psi_rec: psi::Receiver,
    psi_sed: psi::Sender,
    data_r: Vec<psi::Point>,
    data_s: Vec<psi::Point>,
) {
    let (done_tx, done_rx) = std_mpsc::channel::<()>();
    let (sender, receiver) = std_mpsc::channel();

    let msg1 = psi_rec.msg(&data_r);

    // Sender thread
    thread::spawn(move || {
        for i in 0..data_s.len() {
            let msg = psi_sed.send_msg_single(&msg1, &data_s[i], i * psi::BLK_CELLS);
            if sender.send(msg).is_err() {
                println!("Receiver has been dropped!");
                break;
            }
        }
    });

    // Receiver thread
    thread::spawn(move || {
        let mut count = 0u32;
        for msg2 in receiver.iter() {
            count += psi_rec.post_process(&msg2);
        }
        println!("count: {}", count);
        done_tx.send(()).expect("Failed to send done signal");
    });

    // Wait for the tasks to finish.
    done_rx.recv().expect("Failed to receive done signal");
}

pub fn run_standard_net(
    mut psi_rec: psi::Receiver,
    psi_sed: psi::Sender,
    data_r: Vec<psi::Point>,
    data_s: Vec<psi::Point>,
) {
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

    let msg1 = psi_rec.msg(&data_r);

    // Sender thread
    let sender_handle = thread::spawn(move || {
        let listener = TcpListener::bind("127.0.0.1:8080").expect("Failed to bind to address");
        let (mut stream, _) = listener.accept().expect("Failed to accept connection");
        let mut total_sent = 0u64;
        for i in 0..data_s.len() {
            let msg = psi_sed.send_msg_single(&msg1, &data_s[i], i * psi::BLK_CELLS);
            let serialized_msg = bincode::serialize(&msg).expect("Failed to serialize message");
            total_sent += serialized_msg.len() as u64;
            stream.write_all(&serialized_msg).expect("Failed to send message");
        }

        total_sent += bincode::serialize(&msg1).expect("Failed to serialize message").len() as u64;
        println!("Total sent: {} bytes, {} KiB, {} MiB", total_sent, total_sent / 1024, total_sent / (1024 * 1024));
    });

    // Receiver thread
    let receiver_handle = thread::spawn(move || {
        let mut stream = TcpStream::connect("127.0.0.1:8080").expect("Failed to connect to receiver");
        let mut count = 0u32;
        let mut buffer = Vec::new();
        // let mut total_received = 0u64;

        loop {
            let mut temp_buf = [0; 1024 * 1024];
            match stream.read(&mut temp_buf) {
                Ok(0) => break, // Connection closed
                Ok(n) => {
                    // println!("{}", n);
                    // total_received += n as u64;
                    buffer.extend_from_slice(&temp_buf[..n]);
                    while let Ok(msg) = bincode::deserialize::<okvs::Encoding>(&buffer) {
                        count += psi_rec.post_process(&msg);
                        buffer.drain(..bincode::serialized_size(&msg).unwrap() as usize);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read from socket: {}", e);
                    break;
                }
            }
        }

        println!("count: {}", count);
        // println!("Total received: {} bytes, {} KiB, {} MiB", total_received, total_received / 1024, total_received / (1024 * 1024));
        done_tx.send(()).expect("Failed to send done signal");
    });

    // Wait for the tasks to finish.
    done_rx.recv().expect("Failed to receive done signal");

    // Join threads to ensure they complete
    sender_handle.join().expect("Sender thread panicked");
    receiver_handle.join().expect("Receiver thread panicked");
}

pub fn run_standard_apart(
    mut psi_rec: psi::Receiver,
    psi_sed: psi::Sender,
    data_r: Vec<psi::Point>,
    data_s: Vec<psi::Point>,
) {
    let (done_tx, done_rx) = std_mpsc::channel::<()>();
    let (sender, receiver) = std_mpsc::channel();

    let msg1 = psi_rec.msg_apart(&data_r);

    // Sender thread
    thread::spawn(move || {
        for i in 0..data_s.len() {
            let msg = psi_sed.send_msg_single_apart(&msg1, &data_s[i], i);
            if sender.send(msg).is_err() {
                println!("Receiver has been dropped!");
                break;
            }
        }
    });

    // Receiver thread
    thread::spawn(move || {
        let mut count = 0u32;
        for msg2 in receiver.iter() {
            count += psi_rec.post_process_apart(&msg2);
        }
        println!("count: {}", count);
        done_tx.send(()).expect("Failed to send done signal");
    });

    // Wait for the tasks to finish.
    done_rx.recv().expect("Failed to receive done signal");
}

pub fn run_apart_net(
    mut psi_rec: psi::Receiver,
    psi_sed: psi::Sender,
    data_r: Vec<psi::Point>,
    data_s: Vec<psi::Point>,
) {
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

    let msg1 = psi_rec.msg_apart(&data_r);

    // Sender thread
    let sender_handle = thread::spawn(move || {
        let listener = TcpListener::bind("127.0.0.1:8080").expect("Failed to bind to address");
        let (mut stream, _) = listener.accept().expect("Failed to accept connection");
        let mut total_sent = 0u64;
        for i in 0..data_s.len() {
            let msg = psi_sed.send_msg_single_apart(&msg1, &data_s[i], i);
            let serialized_msg = bincode::serialize(&msg).expect("Failed to serialize message");
            total_sent += serialized_msg.len() as u64;
            stream.write_all(&serialized_msg).expect("Failed to send message");
        }

        total_sent += bincode::serialize(&msg1).expect("Failed to serialize message").len() as u64;
        println!("Total sent: {} bytes, {} KiB, {} MiB", total_sent, total_sent / 1024, total_sent / (1024 * 1024));
    });

    // Receiver thread
    let receiver_handle = thread::spawn(move || {
        let mut stream = TcpStream::connect("127.0.0.1:8080").expect("Failed to connect to receiver");
        let mut count = 0u32;
        let mut buffer = Vec::new();
        // let mut total_received = 0u64;

        loop {
            let mut temp_buf = [0; 1024 * 1024];
            match stream.read(&mut temp_buf) {
                Ok(0) => break, // Connection closed
                Ok(n) => {
                    // println!("{}", n);
                    // total_received += n as u64;
                    buffer.extend_from_slice(&temp_buf[..n]);
                    while let Ok(msg) = bincode::deserialize::<PointPair>(&buffer) {
                        count += psi_rec.post_process_apart(&msg);
                        buffer.drain(..bincode::serialized_size(&msg).unwrap() as usize);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read from socket: {}", e);
                    break;
                }
            }
        }

        println!("count: {}", count);
        // println!("Total received: {} bytes, {} KiB, {} MiB", total_received, total_received / 1024, total_received / (1024 * 1024));
        done_tx.send(()).expect("Failed to send done signal");
    });

    // Wait for the tasks to finish.
    done_rx.recv().expect("Failed to receive done signal");

    // Join threads to ensure they complete
    sender_handle.join().expect("Sender thread panicked");
    receiver_handle.join().expect("Receiver thread panicked");
}

pub fn run_standard_lp(
    mut psi_rec: psi::Receiver,
    psi_sed: psi::Sender,
    data_r: Vec<psi::Point>,
    data_s: Vec<psi::Point>,
    metric: u32,
) {
    let (done_tx, done_rx) = std_mpsc::channel::<()>();
    let (sender, receiver) = std_mpsc::channel();

    let msg1 = psi_rec.lp_msg_apart(&data_r, metric);

    // Sender thread
    thread::spawn(move || {
        for i in 0..data_s.len() {
            let msg = psi_sed.lp_send_msg_single_apart(&msg1, &data_s[i], i);
            if sender.send(msg).is_err() {
                println!("Receiver has been dropped!");
                break;
            }
        }
    });

    // Receiver thread
    thread::spawn(move || {
        let mut count = 0u32;
        for msg2 in receiver.iter() {
            count += psi_rec.lp_post_process_apart(&msg2);
        }
        println!("Lp metric {}, count: {}", metric, count);
        done_tx.send(()).expect("Failed to send done signal");
    });

    // Wait for the tasks to finish.
    done_rx.recv().expect("Failed to receive done signal");
}
