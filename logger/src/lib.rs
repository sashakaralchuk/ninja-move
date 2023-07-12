pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

// This is a really bad adding function, its purpose is to fail in this
// example.
#[allow(dead_code)]
fn bad_add(a: i32, b: i32) -> i32 {
    a - b
}


#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(add(1, 2), 3);
    }

    #[test]
    #[should_panic]
    fn test_bad_add() {
        // This assert would fire and test will fail.
        // Please note, that private functions can be tested too!
        assert_eq!(bad_add(1, 2), 3);
    }
}



// use std::time::SystemTime;


// // millis in day
// const DAY_MILLIS: u128 = 1000*60*60*24;


// pub struct Facade {
//     day_threshold: u128,
// }


// impl Facade {
//     fn _now() -> u128 {
//         SystemTime::now()
//             .duration_since(SystemTime::UNIX_EPOCH)
//             .unwrap()
//             .as_millis()
//     }

//     fn _day_threshold() -> u128 {
//         let now = Self::_now();
//         (1 + now / DAY_MILLIS) * DAY_MILLIS
//     }

//     pub fn new() -> Self {
//         let day_threshold = Self::_day_threshold();
//         Facade{day_threshold}
//     }

//     pub fn info(&self, message: &str) {
//         let now = Self::_now();
//         if now > self.day_threshold {
//             println!("day ended, re-generate loggers");
//         }
//         println!("log::info!({})", message);
//     }

//     pub fn dumb() -> &'static str {
//         return "lala"
//     }
// }

