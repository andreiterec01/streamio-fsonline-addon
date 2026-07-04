use bytes::Bytes;
use mpeg2ts_reader::demultiplex;
use mpeg2ts_reader::demultiplex::DemuxContext;
use mpeg2ts_reader::packet::Packet;
use mpeg2ts_reader::pes::PesPacketFilter;
use mpeg2ts_reader::psi;

pub struct TsStartTimeParser {
    last_packet: Bytes,
    ctx: PcrDumpDemuxContext,
    demux: demultiplex::Demultiplex<PcrDumpDemuxContext>,
}

impl TsStartTimeParser {
    pub fn new() -> Self {
        let mut ctx = PcrDumpDemuxContext::new();

        // create the demultiplexer, which will use the ctx to create a filter for pid 0 (PAT)
        let demux = demultiplex::Demultiplex::new(&mut ctx);

        Self {
            ctx,
            demux,
            last_packet: Bytes::new(),
        }
    }

    pub fn parse_packets(&mut self, mut packet: Bytes) -> Option<f32> {
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

        self.ctx.seconds
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
#[derive(Default)]
pub struct PcrDumpDemuxContext {
    changeset: demultiplex::FilterChangeset<PcrDumpFilterSwitch>,
    seconds: Option<f32>,
}
impl PcrDumpDemuxContext {
    fn new() -> Self {
        Self::default()
    }
}

impl DemuxContext for PcrDumpDemuxContext {
    type F = PcrDumpFilterSwitch;

    fn filter_changeset(&mut self) -> &mut demultiplex::FilterChangeset<Self::F> {
        &mut self.changeset
    }

    fn construct(&mut self, req: demultiplex::FilterRequest<'_, '_>) -> PcrDumpFilterSwitch {
        if self.seconds.is_some() {
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
    fn start_stream(&mut self, ctx: &mut PcrDumpDemuxContext) {}
    fn begin_packet(
        &mut self,
        ctx: &mut PcrDumpDemuxContext,
        header: mpeg2ts_reader::pes::PesHeader<'_>,
    ) {
        if ctx.seconds.is_some() {
            return;
        }
        if let mpeg2ts_reader::pes::PesContents::Parsed(Some(parsed)) = header.contents() {
            let pts = parsed.pts_dts().unwrap();

            match pts {
                mpeg2ts_reader::pes::PtsDts::PtsOnly(Ok(pts))
                | mpeg2ts_reader::pes::PtsDts::Both { pts: Ok(pts), .. } => {
                    println!("PTS: {}", pts.value());
                    let seconds = pts.value() as f32 / 90_000.0;
                    println!("Seconds: {:.3}", seconds);
                    ctx.seconds = Some(seconds);
                }
                _ => {}
            }
        }
    }

    fn continue_packet(&mut self, ctx: &mut PcrDumpDemuxContext, data: &[u8]) {}
    fn continuity_error(&mut self, ctx: &mut PcrDumpDemuxContext) {
        println!("Continuity error");
    }
    fn end_packet(&mut self, ctx: &mut PcrDumpDemuxContext) {}
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use mpeg2ts_reader::packet::Packet;

    use crate::ts_parser::TsStartTimeParser;

    #[test]
    fn test_ts_parser() {
        let seg = include_bytes!("../test_files/seg.ts");
        assert_eq!(seg.len() % Packet::SIZE, 0);
        let mut parser = TsStartTimeParser::new();
        for chunk in seg.chunks(Packet::SIZE - 1) {
            let r = parser.parse_packets(Bytes::from_static(chunk));
            if r.is_some() {
                return;
            }
        }

        panic!("Didn't received the start time");
    }
}
