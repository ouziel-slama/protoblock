use bytes::{Bytes, BytesMut};
use std::borrow::Cow;
use std::rc::Rc;
use std::sync::Arc;

/// Trait describing how many bytes a value contributes to the queue budget.
///
/// Implement this for your protocol's `PreProcessed` type so the queue can track real memory usage
/// instead of relying on the raw RPC payload length.
pub trait QueueByteSize {
    /// Estimate the number of bytes retained when this value is enqueued.
    fn queue_bytes(&self) -> usize;
}

impl QueueByteSize for () {
    fn queue_bytes(&self) -> usize {
        0
    }
}

impl QueueByteSize for bool {
    fn queue_bytes(&self) -> usize {
        core::mem::size_of::<bool>()
    }
}

impl QueueByteSize for u8 {
    fn queue_bytes(&self) -> usize {
        1
    }
}

impl QueueByteSize for i8 {
    fn queue_bytes(&self) -> usize {
        1
    }
}

macro_rules! impl_scalar_queue_bytes {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl QueueByteSize for $ty {
                fn queue_bytes(&self) -> usize {
                    core::mem::size_of::<$ty>()
                }
            }
        )+
    };
}

impl_scalar_queue_bytes!(u16, u32, u64, u128, usize, i16, i32, i64, i128, isize, f32, f64);

impl<T: QueueByteSize + ?Sized> QueueByteSize for &T {
    fn queue_bytes(&self) -> usize {
        T::queue_bytes(self)
    }
}

impl<T: QueueByteSize + ?Sized> QueueByteSize for Box<T> {
    fn queue_bytes(&self) -> usize {
        (**self).queue_bytes()
    }
}

impl<T: QueueByteSize + ?Sized> QueueByteSize for Arc<T> {
    fn queue_bytes(&self) -> usize {
        (**self).queue_bytes()
    }
}

impl<T: QueueByteSize + ?Sized> QueueByteSize for Rc<T> {
    fn queue_bytes(&self) -> usize {
        (**self).queue_bytes()
    }
}

impl<T: QueueByteSize> QueueByteSize for Option<T> {
    fn queue_bytes(&self) -> usize {
        self.as_ref().map(QueueByteSize::queue_bytes).unwrap_or(0)
    }
}

impl<T: QueueByteSize, E: QueueByteSize> QueueByteSize for Result<T, E> {
    fn queue_bytes(&self) -> usize {
        match self {
            Ok(value) => value.queue_bytes(),
            Err(err) => err.queue_bytes(),
        }
    }
}

impl QueueByteSize for [u8] {
    fn queue_bytes(&self) -> usize {
        self.len()
    }
}

impl QueueByteSize for Bytes {
    fn queue_bytes(&self) -> usize {
        self.len()
    }
}

impl QueueByteSize for BytesMut {
    fn queue_bytes(&self) -> usize {
        self.len()
    }
}

impl<'a> QueueByteSize for Cow<'a, [u8]> {
    fn queue_bytes(&self) -> usize {
        self.len()
    }
}

impl QueueByteSize for String {
    fn queue_bytes(&self) -> usize {
        self.len()
    }
}

impl QueueByteSize for str {
    fn queue_bytes(&self) -> usize {
        self.len()
    }
}

impl<T: QueueByteSize> QueueByteSize for Vec<T> {
    fn queue_bytes(&self) -> usize {
        self.iter()
            .fold(0usize, |acc, item| acc.saturating_add(item.queue_bytes()))
            .saturating_add(core::mem::size_of::<usize>() * 3)
    }
}

impl<T: QueueByteSize, const N: usize> QueueByteSize for [T; N] {
    fn queue_bytes(&self) -> usize {
        self.iter()
            .fold(0usize, |acc, item| acc.saturating_add(item.queue_bytes()))
    }
}

macro_rules! impl_tuple_queue_bytes {
    ($($name:ident),+ $(,)?) => {
        impl<$($name: QueueByteSize),+> QueueByteSize for ($($name,)+) {
            fn queue_bytes(&self) -> usize {
                #[allow(non_snake_case)]
                let ($($name,)+) = self;
                0usize $(.saturating_add($name.queue_bytes()))+
            }
        }
    };
}

impl_tuple_queue_bytes!(A, B);
impl_tuple_queue_bytes!(A, B, C);
impl_tuple_queue_bytes!(A, B, C, D);
impl_tuple_queue_bytes!(A, B, C, D, E);

#[cfg(test)]
mod tests {
    use super::QueueByteSize;
    use bytes::Bytes;

    #[test]
    fn measures_collections_and_tuples() {
        let buffer = vec![0u8; 128];
        assert_eq!(
            buffer
                .queue_bytes()
                .saturating_sub(core::mem::size_of::<usize>() * 3),
            128
        );

        let tuple = (
            "abc".to_string(),
            vec![1u8, 2, 3],
            Bytes::from_static(b"xyz"),
        );
        assert_eq!(
            tuple.queue_bytes(),
            3 + (3 + core::mem::size_of::<usize>() * 3) + 3
        );
    }

    #[test]
    fn option_and_result_delegate_to_inner_values() {
        let value: Option<Vec<u8>> = Some(vec![1, 2, 3, 4]);
        assert!(value.queue_bytes() >= 4);

        let result: Result<Vec<u8>, Vec<u8>> = Err(vec![5; 16]);
        assert!(result.queue_bytes() >= 16);
    }
}
