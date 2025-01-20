#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let iters = iters
            .into_iter()
            .filter(|storage_iter| storage_iter.is_valid())
            .enumerate()
            .map(|(key, value)| HeapWrapper(key, value))
            .collect::<Vec<_>>();
        let mut iters = BinaryHeap::from(iters);
        let current = iters.pop();

        MergeIterator { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        match self.current.as_ref() {
            Some(wrapper) => {
                let iter = &wrapper.1;
                iter.key()
            }
            None => KeySlice::from_slice(&[]),
        }
    }

    fn value(&self) -> &[u8] {
        match self.current.as_ref() {
            Some(wrapper) => {
                let iter = &wrapper.1;
                iter.value()
            }
            None => &[],
        }
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map_or(false, |iter| iter.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let current = match self.current.as_mut() {
            None => return Ok(()),
            Some(current) => current,
        };

        let key = current.1.key();

        while let Some(mut inner) = self.iters.peek_mut() {
            if inner.1.key() == key {
                if let e @ Err(_) = inner.1.next() {
                    PeekMut::pop(inner);
                    return e;
                }
                if !inner.1.is_valid() {
                    PeekMut::pop(inner);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }
}
