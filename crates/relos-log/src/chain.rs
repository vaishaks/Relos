use relos_core::{LogPos, LOG_POS_BEGIN};
use serde::{Deserialize, Serialize};

/// A segment in the log chain mapping a global position range to a loglet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainSegment {
    /// Identifier for the loglet backing this segment.
    pub loglet_id: String,
    /// Global start position (inclusive).
    pub start_pos: LogPos,
    /// Global end position (exclusive). None means this is the active/open segment.
    pub end_pos: Option<LogPos>,
}

/// The full log chain: an ordered sequence of segments.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogChain {
    pub segments: Vec<ChainSegment>,
}

impl LogChain {
    /// Create a new log chain with a single open segment starting at LOG_POS_BEGIN.
    pub fn new(initial_loglet_id: String) -> Self {
        Self {
            segments: vec![ChainSegment {
                loglet_id: initial_loglet_id,
                start_pos: LOG_POS_BEGIN,
                end_pos: None,
            }],
        }
    }

    /// Returns the active (last open) segment, i.e., the last segment with end_pos = None.
    pub fn active_segment(&self) -> Option<&ChainSegment> {
        self.segments.last().filter(|s| s.end_pos.is_none())
    }

    /// Find the segment containing the given global position.
    pub fn find_segment(&self, global_pos: LogPos) -> Option<&ChainSegment> {
        self.segments.iter().find(|s| {
            global_pos >= s.start_pos
                && match s.end_pos {
                    Some(end) => global_pos < end,
                    None => true,
                }
        })
    }

    /// Seal the active segment at `seal_pos` and append a new segment with the given loglet.
    pub fn extend(&mut self, seal_pos: LogPos, new_loglet_id: String) {
        // Seal the current active segment
        if let Some(last) = self.segments.last_mut() {
            if last.end_pos.is_none() {
                last.end_pos = Some(seal_pos);
            }
        }
        // Append a new open segment
        self.segments.push(ChainSegment {
            loglet_id: new_loglet_id,
            start_pos: seal_pos,
            end_pos: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_chain() {
        let chain = LogChain::new("loglet-0".to_string());
        assert_eq!(chain.segments.len(), 1);
        assert_eq!(chain.segments[0].loglet_id, "loglet-0");
        assert_eq!(chain.segments[0].start_pos, LOG_POS_BEGIN);
        assert!(chain.segments[0].end_pos.is_none());
    }

    #[test]
    fn test_active_segment() {
        let chain = LogChain::new("loglet-0".to_string());
        let active = chain.active_segment().unwrap();
        assert_eq!(active.loglet_id, "loglet-0");
    }

    #[test]
    fn test_find_segment_single() {
        let chain = LogChain::new("loglet-0".to_string());
        let seg = chain.find_segment(1).unwrap();
        assert_eq!(seg.loglet_id, "loglet-0");

        let seg = chain.find_segment(100).unwrap();
        assert_eq!(seg.loglet_id, "loglet-0");
    }

    #[test]
    fn test_extend() {
        let mut chain = LogChain::new("loglet-0".to_string());
        chain.extend(5, "loglet-1".to_string());

        assert_eq!(chain.segments.len(), 2);
        assert_eq!(chain.segments[0].end_pos, Some(5));
        assert_eq!(chain.segments[1].start_pos, 5);
        assert!(chain.segments[1].end_pos.is_none());
    }

    #[test]
    fn test_find_segment_after_extend() {
        let mut chain = LogChain::new("loglet-0".to_string());
        chain.extend(5, "loglet-1".to_string());

        // Positions 1-4 should map to loglet-0
        assert_eq!(chain.find_segment(1).unwrap().loglet_id, "loglet-0");
        assert_eq!(chain.find_segment(4).unwrap().loglet_id, "loglet-0");

        // Position 5+ should map to loglet-1
        assert_eq!(chain.find_segment(5).unwrap().loglet_id, "loglet-1");
        assert_eq!(chain.find_segment(100).unwrap().loglet_id, "loglet-1");
    }

    #[test]
    fn test_active_segment_after_extend() {
        let mut chain = LogChain::new("loglet-0".to_string());
        chain.extend(5, "loglet-1".to_string());

        let active = chain.active_segment().unwrap();
        assert_eq!(active.loglet_id, "loglet-1");
    }

    #[test]
    fn test_multiple_extends() {
        let mut chain = LogChain::new("loglet-0".to_string());
        chain.extend(5, "loglet-1".to_string());
        chain.extend(10, "loglet-2".to_string());

        assert_eq!(chain.segments.len(), 3);
        assert_eq!(chain.find_segment(3).unwrap().loglet_id, "loglet-0");
        assert_eq!(chain.find_segment(7).unwrap().loglet_id, "loglet-1");
        assert_eq!(chain.find_segment(15).unwrap().loglet_id, "loglet-2");
    }
}
