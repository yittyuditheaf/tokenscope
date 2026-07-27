fn main() {
    let source = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "claude".to_string());
    println!("{}", tokenscope_lib::dashboard_json_for(&source));
}
