/// XXX: should be an intergration test
#[test]
fn intergration_add() {
    assert_eq!(logger::add(3, 2), 5);
}