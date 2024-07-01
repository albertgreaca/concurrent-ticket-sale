//! 🏗 HTTP request implementation

use std::io;
use std::io::{Read, Write};

use ticket_sale_core::RequestKind;
use tiny_http::{Header, Response};
use uuid::Uuid;

/// Length of any hyphenated UUID
const UUID_LEN: usize = b"a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8".len();

struct HTTPRequest(tiny_http::Request);

impl ticket_sale_core::RawRequest for HTTPRequest {
    fn url(&self) -> &str {
        self.0.url()
    }

    fn method(&self) -> ticket_sale_core::RequestMethod {
        match self.0.method() {
            tiny_http::Method::Get => ticket_sale_core::RequestMethod::Get,
            tiny_http::Method::Post => ticket_sale_core::RequestMethod::Post,
            _ => unreachable!(),
        }
    }

    fn read_bytes(&mut self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.0.body_length().unwrap_or(0));
        self.0.as_reader().read_to_end(&mut buf)?;
        Ok(buf)
    }

    fn read_string(&mut self) -> io::Result<String> {
        let mut s = String::with_capacity(self.0.body_length().unwrap_or(0));
        self.0.as_reader().read_to_string(&mut s)?;
        Ok(s)
    }

    fn read_u32(&mut self) -> Option<u32> {
        let mut s = String::with_capacity(self.0.body_length().unwrap_or(16));
        self.0.as_reader().read_to_string(&mut s).ok()?;
        s.parse().ok()
    }

    fn respond_with_err(self: Box<Self>, err: String, customer: Uuid, server: Option<Uuid>) {
        self.respond(
            Response::from_string(err).with_status_code(400),
            customer,
            server,
        )
    }

    fn respond_with_int(self: Box<Self>, int: u32, customer: Uuid, server: Option<Uuid>) {
        self.respond(
            Response::from_string(int.to_string()).with_status_code(200),
            customer,
            server,
        )
    }

    fn respond_with_string(self: Box<Self>, s: String, customer: Uuid, server: Option<Uuid>) {
        self.respond(
            Response::from_string(s).with_status_code(200),
            customer,
            server,
        )
    }

    fn respond_with_sold_out(self: Box<Self>, customer: Uuid, server: Option<Uuid>) {
        self.respond(
            Response::from_string("SOLD OUT").with_status_code(200),
            customer,
            server,
        )
    }

    fn respond_with_server_list(self: Box<Self>, servers: &[Uuid]) {
        let mut s = Vec::<u8>::with_capacity((UUID_LEN + 1) * servers.len());
        for id in servers {
            writeln!(&mut s, "{}", id.hyphenated()).unwrap();
        }

        let mut res = Response::from_data(s);
        add_response_cors_headers(&mut res);
        self.0.respond(res).expect("HTTP response failed");
    }
}

impl HTTPRequest {
    /// Add HTTP headers (CORS, X-Customer-Id, X-Server-Id) to `res` and send it
    fn respond<R: Read>(self, mut res: Response<R>, customer: Uuid, server: Option<Uuid>) {
        add_response_cors_headers(&mut res);

        let mut cid = Vec::<u8>::with_capacity(UUID_LEN);
        write!(&mut cid, "{}", customer.hyphenated()).unwrap();
        res.add_header(tiny_http::Header::from_bytes(b"X-Customer-Id", cid).unwrap());

        if let Some(server) = server {
            let mut sid = Vec::<u8>::with_capacity(UUID_LEN);
            write!(&mut sid, "{}", server.hyphenated()).unwrap();
            res.add_header(tiny_http::Header::from_bytes(b"X-Server-Id", sid).unwrap());
        }

        self.0.respond(res).expect("HTTP response failed");
    }
}

/// Parse the given HTTP request
///
/// If [`None`] is returned, the request was already answered with a
/// corresponding error message.
pub fn parse(rq: tiny_http::Request) -> Option<ticket_sale_core::Request> {
    use tiny_http::Method::*;

    let kind = match (rq.method(), rq.url()) {
        (Options, _) => {
            let mut res = Response::empty(204);
            add_response_cors_headers(&mut res);
            rq.respond(res).expect("HTTP response failed");
            return None;
        }
        (Get, "/api/admin/num_servers") => RequestKind::GetNumServers,
        (Post, "/api/admin/num_servers") => RequestKind::SetNumServers,
        (Get, "/api/admin/get_servers") => RequestKind::GetServers,
        (Get, "/api/num_available_tickets") => RequestKind::NumAvailableTickets,
        (Post, "/api/reserve_ticket") => RequestKind::ReserveTicket,
        (Post, "/api/buy_ticket") => RequestKind::BuyTicket,
        (Post, "/api/abort_purchase") => RequestKind::AbortPurchase,
        (Get, url) | (Post, url) => {
            if url.starts_with("/api/debug") {
                RequestKind::Debug
            } else {
                let mut res = Response::from_string(
                    "🦀 could not find the service you are looking for!

Valid requests are:
  GET  /api/admin/num_servers
  POST /api/admin/num_servers
  GET  /api/admin/get_servers
  GET  /api/num_available_tickets
  POST /api/reserve_ticket
  POST /api/buy_ticket
  POST /api/abort_purchase
  GET  /api/debug(.*)
  POST /api/debug(.*)",
                )
                .with_status_code(404);
                add_response_cors_headers(&mut res);
                rq.respond(res).expect("HTTP response failed");
                return None;
            }
        }
        _ => {
            let mut res = Response::empty(405);
            add_response_cors_headers(&mut res);
            rq.respond(res).expect("HTTP response failed");
            return None;
        }
    };

    let mut cid = None;
    let mut sid = None;
    for hdr in rq.headers() {
        if hdr.field.equiv("x-server-id") {
            if let Ok(id) = Uuid::parse_str(hdr.value.as_str()) {
                sid = Some(id);
            }
        } else if hdr.field.equiv("x-customer-id") {
            if let Ok(id) = Uuid::parse_str(hdr.value.as_str()) {
                cid = Some(id);
            }
        }
    }

    Some(ticket_sale_core::Request::from_raw(
        kind,
        cid.unwrap_or_else(Uuid::new_v4),
        sid,
        Box::new(HTTPRequest(rq)),
    ))
}

/// Add CORS headers to `res`
fn add_response_cors_headers<R: Read>(res: &mut Response<R>) {
    res.add_header(Header::from_bytes(b"Access-Control-Request-Method", b"*").unwrap());
    res.add_header(Header::from_bytes(b"Access-Control-Allow-Origin", b"*").unwrap());
    res.add_header(Header::from_bytes(b"Access-Control-Allow-Headers", b"*").unwrap());
    res.add_header(Header::from_bytes(b"Access-Control-Expose-Headers", b"*").unwrap());
}
