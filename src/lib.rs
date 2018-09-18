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

extern crate amqp;

extern crate futures;
extern crate lapin_futures;
extern crate amq_protocol;
extern crate tokio;
extern crate tokio_io;

pub type Payload = (String, Vec<u8>);

pub mod v1;
pub mod v2;
