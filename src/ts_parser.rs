use bytes::Bytes;
use mpeg2ts_reader::demultiplex;
use mpeg2ts_reader::demultiplex::DemuxContext;
use mpeg2ts_reader::packet::Packet;
use mpeg2ts_reader::pes::PesPacketFilter;
use mpeg2ts_reader::psi;

pub struct TsTimeParser {
    last_packet: Bytes,
    ctx: PcrDumpDemuxContext,
    demux: demultiplex::Demultiplex<PcrDumpDemuxContext>,
}

impl TsTimeParser {
    pub fn new(only_start_time: bool) -> Self {
        let mut ctx = PcrDumpDemuxContext::new(only_start_time);

        // create the demultiplexer, which will use the ctx to create a filter for pid 0 (PAT)
        let demux = demultiplex::Demultiplex::new(&mut ctx);

        Self {
            ctx,
            demux,
            last_packet: Bytes::new(),
        }
    }

    pub fn parse_packets(&mut self, mut packet: Bytes) {
        let remaining = if !self.last_packet.is_empty() {
            let remaining = Packet::SIZE - self.last_packet.len();
            if remaining < packet.len() {
                let mut new_packet = self.last_packet.to_vec();
                new_packet.extend(packet);
                let new_packet = Bytes::from(new_packet);
                self.last_packet.clear();
                return self.parse_packets(new_packet);
            }
            let mut first_packet = self.last_packet.to_vec();
            first_packet.extend(&packet[..remaining]);

            self.demux.push(&mut self.ctx, &first_packet);
            remaining
        } else {
            0
        };
        let packet_bytes = &packet[remaining..];
        let packet_len = packet_bytes.len() / Packet::SIZE * Packet::SIZE;
        self.demux.push(&mut self.ctx, &packet_bytes[..packet_len]);

        self.last_packet = packet.split_off(remaining + packet_len);
    }

    pub fn parse_and_return_start_time(&mut self, packet: Bytes) -> Option<f32> {
        self.parse_packets(packet);
        self.start_time()
    }

    pub fn start_time(&self) -> Option<f32> {
        self.ctx.seconds_first
    }

    pub fn end_time(&self) -> Option<f32> {
        self.ctx.seconds_last
    }
}

mpeg2ts_reader::packet_filter_switch! {
    PcrDumpFilterSwitch<PcrDumpDemuxContext> {
        Pat: demultiplex::PatPacketFilter<PcrDumpDemuxContext>,
        Pmt: demultiplex::PmtPacketFilter<PcrDumpDemuxContext>,
        Null: demultiplex::NullPacketFilter<PcrDumpDemuxContext>,
        // Pcr: PcrPacketFilter<PcrDumpDemuxContext>,
        Pes: PesPacketFilter<PcrDumpDemuxContext, FirstPtsConsumer>,
    }
}
pub struct PcrDumpDemuxContext {
    changeset: demultiplex::FilterChangeset<PcrDumpFilterSwitch>,
    seconds_first: Option<f32>,
    seconds_last: Option<f32>,
    only_start_time: bool,
}
impl PcrDumpDemuxContext {
    fn new(only_start_time: bool) -> Self {
        Self {
            changeset: demultiplex::FilterChangeset::default(),
            seconds_first: None,
            seconds_last: None,
            only_start_time,
        }
    }
}

impl DemuxContext for PcrDumpDemuxContext {
    type F = PcrDumpFilterSwitch;

    fn filter_changeset(&mut self) -> &mut demultiplex::FilterChangeset<Self::F> {
        &mut self.changeset
    }

    fn construct(&mut self, req: demultiplex::FilterRequest<'_, '_>) -> PcrDumpFilterSwitch {
        if self.only_start_time && self.seconds_first.is_some() {
            return PcrDumpFilterSwitch::Null(demultiplex::NullPacketFilter::default());
        }
        match req {
            demultiplex::FilterRequest::ByPid(psi::pat::PAT_PID) => {
                PcrDumpFilterSwitch::Pat(demultiplex::PatPacketFilter::default())
                // PcrDumpFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::Pmt {
                pid,
                program_number,
            } => PcrDumpFilterSwitch::Pmt(demultiplex::PmtPacketFilter::new(pid, program_number)),

            demultiplex::FilterRequest::ByStream {
                pmt, stream_info, ..
            } => {
                if stream_info.elementary_pid() == pmt.pcr_pid() {
                    PcrDumpFilterSwitch::Pes(PesPacketFilter::new(FirstPtsConsumer))
                    // PcrDumpFilterSwitch::Pcr(PcrPacketFilter::construct(pmt, stream_info))
                } else {
                    PcrDumpFilterSwitch::Null(demultiplex::NullPacketFilter::default())
                }
            }

            demultiplex::FilterRequest::ByPid(_) => {
                PcrDumpFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::Nit { .. } => {
                PcrDumpFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
        }
    }
}

struct FirstPtsConsumer;

impl mpeg2ts_reader::pes::ElementaryStreamConsumer<PcrDumpDemuxContext> for FirstPtsConsumer {
    fn start_stream(&mut self, _ctx: &mut PcrDumpDemuxContext) {}
    fn begin_packet(
        &mut self,
        ctx: &mut PcrDumpDemuxContext,
        header: mpeg2ts_reader::pes::PesHeader<'_>,
    ) {
        if ctx.only_start_time && ctx.seconds_first.is_some() {
            return;
        }
        if let mpeg2ts_reader::pes::PesContents::Parsed(Some(parsed)) = header.contents() {
            let pts = parsed.pts_dts().unwrap();

            match pts {
                mpeg2ts_reader::pes::PtsDts::PtsOnly(Ok(pts))
                | mpeg2ts_reader::pes::PtsDts::Both { pts: Ok(pts), .. } => {
                    let seconds = pts.value() as f32 / 90_000.0;
                    ctx.seconds_last = Some(seconds);
                    if ctx.seconds_first.is_none() {
                        ctx.seconds_first = Some(seconds);
                    }
                }
                _ => {}
            }
        }
    }

    fn continue_packet(&mut self, _ctx: &mut PcrDumpDemuxContext, _data: &[u8]) {}
    fn continuity_error(&mut self, _ctx: &mut PcrDumpDemuxContext) {
        tracing::error!("Continuity error");
    }
    fn end_packet(&mut self, _ctx: &mut PcrDumpDemuxContext) {}
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use mpeg2ts_reader::packet::Packet;

    use crate::ts_parser::TsTimeParser;

    #[test]
    fn test_ts_parser() {
        let seg = include_bytes!("../test_files/seg.ts");
        assert_eq!(seg.len() % Packet::SIZE, 0);
        let mut parser = TsTimeParser::new(true);
        for chunk in seg.chunks(Packet::SIZE - 1) {
            parser.parse_packets(Bytes::from_static(chunk));
            if parser.start_time().is_some() {
                return;
            }
        }

        panic!("Didn't received the start time");
    }

    #[test]
    fn test_ts_parser_end_time() {
        let seg = include_bytes!("../test_files/seg.ts");
        assert_eq!(seg.len() % Packet::SIZE, 0);
        let mut parser = TsTimeParser::new(false);
        for chunk in seg.chunks(Packet::SIZE - 1) {
            parser.parse_packets(Bytes::from_static(chunk));
        }

        let start_time = parser.start_time().unwrap();
        let end_time = parser.end_time().unwrap();
        assert!(
            end_time - start_time > 10.,
            "End time({end_time:.2}) should be greater than start time({start_time:.2}) by at least 10 seconds"
        );
    }
}
