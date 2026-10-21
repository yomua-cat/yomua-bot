//! 可恢复的同步原语辅助函数。

use std::sync::{Mutex, MutexGuard};

/// 获取互斥锁；锁中毒时记录错误并继续使用其中保留下来的数据。
pub fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| {
        tracing::error!("互斥锁中毒，恢复内部数据继续运行");
        error.into_inner()
    })
}
