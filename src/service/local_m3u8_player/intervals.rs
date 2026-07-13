use std::collections::{BinaryHeap, HashSet};

use crate::service::local_m3u8_player::OneSegmentTime;

#[derive(Debug)]
pub struct Item {
    segments: std::ops::Range<usize>,
    pub(super) segment_start_time: f32,
    pub(super) segment_end_time: f32,
}
impl Item {
    pub fn duration(&self) -> f32 {
        (self.segment_end_time - self.segment_start_time).max(0.)
    }
    fn is_empty(&self, forbidden: &HashSet<usize>) -> bool {
        if self.segments.is_empty() {
            return true;
        }
        !self
            .segments
            .clone()
            .into_iter()
            .any(|index| !forbidden.contains(&index))
    }

    fn middle_index(&self, forbidden: &HashSet<usize>) -> usize {
        let len = self.segments.end - self.segments.start;
        let mid = self.segments.start + len / 2;
        if !forbidden.contains(&mid) {
            return mid;
        }
        for dir in 1..len.div_ceil(2) {
            if let Some(index) = mid.checked_sub(dir)
                && self.segments.contains(&index)
                && !forbidden.contains(&index)
            {
                return index;
            }

            if let Some(index) = mid.checked_add(dir)
                && self.segments.contains(&index)
                && !forbidden.contains(&index)
            {
                return index;
            }
        }

        panic!("Middle index called on an empty item")
    }
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Item {}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.duration().total_cmp(&other.duration())
    }
}

pub struct Interval {
    ranges: BinaryHeap<Item>,
    forbidden: HashSet<usize>,
}

impl Interval {
    pub fn new(
        segments_count: usize,
        duration: f32,
        initial_segments: impl IntoIterator<Item = OneSegmentTime>,
    ) -> Self {
        let mut ranges = vec![Item {
            segment_start_time: 0.,
            segment_end_time: duration,
            segments: 0..segments_count,
        }];

        let forbidden = HashSet::new();
        for item in initial_segments {
            let (index, _) = ranges
                .iter()
                .enumerate()
                .find(|(_, v)| v.segments.contains(&item.segment_index))
                .expect("A range should be here");
            let element = ranges.swap_remove(index);
            let first_item = Item {
                segment_start_time: element.segment_start_time,
                segment_end_time: item.start_time,
                segments: element.segments.start..item.segment_index,
            };

            let second_item = Item {
                segment_start_time: item.start_time,
                segment_end_time: element.segment_end_time,
                segments: item.segment_index + 1..element.segments.end,
            };
            if !first_item.is_empty(&forbidden) {
                ranges.push(first_item);
            }
            if !second_item.is_empty(&forbidden) {
                ranges.push(second_item);
            }
        }
        Self {
            ranges: ranges.into(),
            forbidden,
        }
    }

    pub fn next_best_to_split(&mut self) -> Option<IntervalItem<'_>> {
        let value = self.ranges.peek()?;
        let segment_index = value.middle_index(&self.forbidden);
        Some(IntervalItem {
            segment_index,
            interval: self,
        })
    }
}

pub struct IntervalItem<'a> {
    segment_index: usize,
    interval: &'a mut Interval,
}

impl<'a> IntervalItem<'a> {
    pub fn item(&self) -> &Item {
        self.interval.ranges.peek().unwrap()
    }

    pub fn index(&self) -> usize {
        self.segment_index
    }

    pub fn split(self, segment_start_time: f32) {
        let element = self
            .interval
            .ranges
            .pop()
            .expect("We know this value should be here");
        let first_item = Item {
            segment_start_time: element.segment_start_time,
            segment_end_time: segment_start_time,
            segments: element.segments.start..self.segment_index,
        };

        let second_item = Item {
            segment_start_time,
            segment_end_time: element.segment_end_time,
            segments: self.segment_index + 1..element.segments.end,
        };
        if !first_item.is_empty(&self.interval.forbidden) {
            self.interval.ranges.push(first_item);
        }
        if !second_item.is_empty(&self.interval.forbidden) {
            self.interval.ranges.push(second_item);
        }
    }

    pub fn remove(self) {
        self.interval.forbidden.insert(self.segment_index);
        if self
            .interval
            .ranges
            .peek()
            .unwrap()
            .is_empty(&self.interval.forbidden)
        {
            self.interval.ranges.pop();
        }
    }
}
