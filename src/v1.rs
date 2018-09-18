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

use std::sync::mpsc::{Receiver, Sender};

use amqp;
use amqp::Basic;

pub use super::Payload;

pub struct Handler {
    tx: Sender<Payload>,
}

impl Handler {
    pub fn new(tx: Sender<Payload>) -> Self {
        Handler { tx: tx }
    }
}

impl amqp::Consumer for Handler {
    fn handle_delivery(
        &mut self,
        channel: &mut amqp::Channel,
        deliver: amqp::protocol::basic::Deliver,
        _: amqp::protocol::basic::BasicProperties,
        body: Vec<u8>,
    ) {
        let _ = self.tx.send((deliver.routing_key, body));
        let _ = channel.basic_ack(deliver.delivery_tag, false);
    }
}

pub fn init_channel(amqp_url: &str) -> amqp::Channel {
    let mut session = match amqp::Session::open_url(amqp_url) {
        Ok(session) => session,
        Err(error) => panic!("Failed to open url {} : {:?}", amqp_url, error),
    };
    let mut channel = session.open_channel(1).ok().expect("Can't open channel");
    let _ = channel.basic_prefetch(10);
    channel
        .exchange_declare(
            "cita",
            "topic",
            false,
            true,
            false,
            false,
            false,
            amqp::Table::new(),
        ).unwrap();
    channel
}

pub fn start(
    amqp_url: &str,
    name: &str,
    keys: &[&str],
    tx: Sender<Payload>,
    rx: Receiver<Payload>,
) {
    {
        let mut channel = init_channel(amqp_url);

        channel
            .queue_declare(
                name.clone(),
                false,
                true,
                false,
                false,
                false,
                amqp::Table::new(),
            ).unwrap();

        for key in keys {
            channel
                .queue_bind(name.clone(), "cita", &key, false, amqp::Table::new())
                .unwrap();
        }

        let callback = Handler::new(tx);
        channel
            .basic_consume(
                callback,
                name.clone(),
                "",
                false,
                false,
                false,
                false,
                amqp::Table::new(),
            ).unwrap();

        let _ = ::std::thread::Builder::new()
            .name("subscriber".to_string())
            .spawn(move || {
                channel.start_consuming();
                let _ = channel.close(200, "Bye");
                ::std::process::exit(0);
            });
    }
    {
        let mut channel = init_channel(amqp_url);
        let _ = ::std::thread::Builder::new()
            .name("publisher".to_string())
            .spawn(move || {
                loop {
                    let ret = rx.recv();
                    if ret.is_err() {
                        break;
                    }
                    let (routing_key, msg) = ret.unwrap();
                    let ret = channel.basic_publish(
                        "cita",
                        &routing_key,
                        false,
                        false,
                        amqp::protocol::basic::BasicProperties {
                            content_type: Some("text".to_string()),
                            ..Default::default()
                        },
                        msg,
                    );
                    if ret.is_err() {
                        break;
                    }
                }
                let _ = channel.close(200, "Bye");
                ::std::process::exit(0);
            });
    }
}
