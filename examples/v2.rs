/*
Copyright (C) 2018 Boyu Yang <yangby@cryptape.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

extern crate rabbitmq_adapter;

use rabbitmq_adapter::v2 as mq_adapter;

fn main() {
    let amqp_url = "amqp://127.0.0.1:5672/vhost2";
    let name = "amqp-test";
    let keys = vec!["amqp-test"];
    let (pub_sender, pub_receiver) = ::std::sync::mpsc::channel();
    let (sub_sender, sub_receiver) = ::std::sync::mpsc::channel();
    mq_adapter::start(amqp_url, name, &keys[..], sub_sender, pub_receiver);

    let _ = ::std::thread::Builder::new()
        .name("publisher".to_string())
        .spawn(move || {
            let mut cnt = 0usize;
            loop {
                if let Ok(_) = sub_receiver.recv() {
                    cnt += 1;
                    if cnt > 0 && cnt % 1000 == 0 {
                        println!("recv {}", cnt);
                    }
                    if cnt >= 50000 {
                        break;
                    }
                }
            }
            ::std::process::exit(0);
        });

    let key = name.to_owned();
    let msg = "msg".as_bytes().to_vec();
    let mut cnt = 0usize;
    loop {
        if cnt < 100000 {
            if pub_sender.send((key.clone(), msg.clone())).is_ok() {
                cnt += 1;
            }
        } else {
            println!("send done");
            sleeping(3_600_000);
        }
    }
}

fn sleeping(millis: u64) {
    let millis = ::std::time::Duration::from_millis(millis);
    ::std::thread::sleep(millis);
}
