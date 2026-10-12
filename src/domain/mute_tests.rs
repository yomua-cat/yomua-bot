//! 静默时段解析与判断的单元测试。

use crate::domain::mute::{is_within_window, parse_mute_schedule, TimeOfDay};

fn time(hour: u32, minute: u32) -> TimeOfDay {
    TimeOfDay { hour, minute }
}

#[test]
fn parse_valid_schedule() {
    let w = parse_mute_schedule("22:00-06:00")
        .expect("合法窗口应解析成功")
        .expect("非空应返回 Some");
    assert_eq!(w.from, time(22, 0));
    assert_eq!(w.until, time(6, 0));
}

#[test]
fn parse_whitespace_or_empty_returns_none() {
    assert!(parse_mute_schedule("").unwrap().is_none());
    assert!(parse_mute_schedule("   ").unwrap().is_none());
    assert!(parse_mute_schedule(" \t ").unwrap().is_none());
}

#[test]
fn parse_invalid_shapes_return_error() {
    assert!(parse_mute_schedule("22:00").is_err(), "缺少分隔符");
    assert!(parse_mute_schedule("22:00-06").is_err(), "缺少分号");
    assert!(parse_mute_schedule("ab:00-06:00").is_err(), "非法小时");
    assert!(parse_mute_schedule("25:00-06:00").is_err(), "小时越界");
    assert!(parse_mute_schedule("22:60-06:00").is_err(), "分钟越界");
}

#[test]
fn within_window_within_same_day() {
    // 09:00-17:00
    let mut w = parse_mute_schedule("09:00-17:00").unwrap().unwrap();
    assert!(is_within_window(&w, &time(9, 0)), "下界包含");
    assert!(is_within_window(&w, &time(12, 30)));
    assert!(!is_within_window(&w, &time(17, 0)), "上界不包含");
    assert!(!is_within_window(&w, &time(8, 59)));

    // 修改为普通窗口再测一次（保留变量可用性）。
    w = parse_mute_schedule("09:00-17:00").unwrap().unwrap();
    assert!(!is_within_window(&w, &time(23, 0)));
}

#[test]
fn within_window_crosses_midnight() {
    // 22:00-06:00 跨午夜。
    let w = parse_mute_schedule("22:00-06:00").unwrap().unwrap();
    assert!(is_within_window(&w, &time(22, 0)));
    assert!(is_within_window(&w, &time(23, 59)));
    assert!(is_within_window(&w, &time(0, 0)));
    assert!(is_within_window(&w, &time(5, 59)));
    assert!(!is_within_window(&w, &time(6, 0)));
    assert!(!is_within_window(&w, &time(12, 0)));
}
