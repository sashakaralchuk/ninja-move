use std::borrow::Borrow;
use std::rc::Rc;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    return bench_rc_vs_arc();
    let _ = TupleStruct;
    let t = Val1 { v1: 1, v2: 2 };
    dbg!(&t);
    let t = vec![1, 2, 3];
    t.to_sql();
    dbg!(&t);
    println!("");
    let _ = ();
    let _ = IpAddrKind::V4.to_sql();
    let _ = IpAddrKind::V6.to_sql();
    let _ = IpAddrKind::TV4(2, 3.0).to_sql();
    let t = IpAddrKind::TV6 { a: 2, b: 3.0 };
    if let IpAddrKind::TV6 { a, b } = t {
        println!("a={}, b={}", a, b);
    }
    let t1: i32 = 2;
    let t2: TTT = 3;
    if t1 == t2 {
        println!("t1 == t2");
    }
    {
        let mut l = LinkedList2::new();
        l.push_left(1);
        l.push_left(44);
        l.push_left(77);
        l.push_left(7);
        l.pop_left();
        println!("{:?}", l.peek());
    }
    {
        let string1 = String::from("long string is long");
        let string2 = String::from("xyz");
        let result;
        {
            result = longest(string1.as_str(), string2.as_str());
        }
        println!("The longest string is {result}");
    }
    execute_cpu_cache_optimization_benchmark();
    {
        let list: article::LinkedListV1<i32> = article::LinkedListV1::<i32>::new();
        let list = list.push_left(1);
        let list = list.push_left(2);
        let list = list.push_left(3);
        let list = list.push_left(4);
        assert_eq!(list.collect(), vec![4, 3, 2, 1]);
    }
    {
        let mut list = article::LinkedListV2::<i32>::new();
        let mut box_1 = article::LinkedListV2::create_box(1);
        let mut box_2 = article::LinkedListV2::create_box(2);
        let mut box_3 = article::LinkedListV2::create_box(3);
        let mut box_4 = article::LinkedListV2::create_box(4);
        list.push_left(&mut box_1);
        list.push_left(&mut box_2);
        list.push_left(&mut box_3);
        list.push_left(&mut box_4);
        assert_eq!(list.collect(), vec![4, 3, 2, 1]);
    }
    {
        let mut list = article::LinkedListV3::<i32>::new();
        list.push_left(1);
        list.push_left(2);
        list.push_left(3);
        list.push_left(4);
        assert_eq!(list.collect(), vec![4, 3, 2, 1]);
        let x = list.pop_left();
        assert_eq!(x.unwrap(), 4);
        let x = list.pop_left();
        assert_eq!(x.unwrap(), 3);
        let x = list.pop_left();
        assert_eq!(x.unwrap(), 2);
    }
    {
        let mut list = article_2_a_bad_stack::List::new();
        list.push(1);
        list.push(2);
        list.push(3);
        list.push(4);
        assert_eq!(list.collect(), vec![4, 3, 2, 1]);
    }
    {
        let mut list = article_2_an_ok_stack::List::new();
        list.push(1);
        list.push(2);
        list.push(3);
        list.push(4);
        assert_eq!(list.collect(), vec![4, 3, 2, 1]);
    }
    {
        let list = article_2_persistent_stack::List::new()
            .prepend(1)
            .prepend(2)
            .prepend(3)
            .prepend(4);
        assert_eq!(list.collect(), vec![4, 3, 2, 1]);
    }
    {
        let mut list = article_2_a_bad_safe_queue::List::new();
        list.push_left(1);
        list.push_left(2);
        list.push_left(3);
        list.push_left(4);
        assert_eq!(list.collect(), vec![4, 3, 2, 1]);
    }
    {
        let mut list = article_2_an_ok_unsafe_queue::List::new();
        list.push_right(1);
        list.push_right(2);
        list.push_right(3);
        list.push_right(4);
        assert_eq!(list.collect(), vec![1, 2, 3, 4]);
    }
    unsafe {
        use std::ptr;
        let mut a = stas_question::A {
            val: 44,
            b: ptr::null_mut(),
        };
        let mut b = stas_question::B {
            val: 77,
            a: ptr::null_mut(),
        };
        a.b = &b as *const stas_question::B;
        b.a = &a as *const stas_question::A;
        println!("a.val={} a.b.val={}", a.val, a.b.as_ref().unwrap().val);
        println!("b.val={} b.a.val={}", b.val, b.a.as_ref().unwrap().val);
    }
    {
        use std::rc::Rc;
        let a = Rc::new(Val1 { v1: 44, v2: 77 });
        let b = Rc::clone(&a);
        println!(
            "red-count=({},{})",
            Rc::strong_count(&a),
            Rc::weak_count(&a)
        );
        {
            let c = Rc::clone(&a);
            println!(
                "red-count=({},{})",
                Rc::strong_count(&a),
                Rc::weak_count(&a)
            );
        }
        println!(
            "red-count=({},{})",
            Rc::strong_count(&a),
            Rc::weak_count(&a)
        );
    }
    {
        use std::cell::RefCell;
        use std::rc::Rc;
        let a = Rc::new(RefCell::new(stas_question::AA { val: 44, b: None }));
        let b = Rc::new(RefCell::new(stas_question::BB {
            val: 77,
            a: Some(Rc::clone(&a)),
        }));
        a.borrow_mut().b = Some(Rc::clone(&b));
        println!(
            "a.val={} a.b.val={}",
            a.as_ref().borrow().val,
            a.as_ref()
                .borrow()
                .b
                .as_ref()
                .unwrap()
                .as_ref()
                .borrow()
                .val,
        );
        println!(
            "b.val={} b.a.val={}",
            b.as_ref().borrow().val,
            b.as_ref()
                .borrow()
                .a
                .as_ref()
                .unwrap()
                .as_ref()
                .borrow()
                .val,
        );
    }
}

fn bench_rc_vs_arc() {
    let mut handles = vec![];
    for _ in 0..10 {
        let handle = std::thread::spawn(move || {
            let start_timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap();
            let c = BenchS { n: 4477 };
            for _ in 0..100_000_000 {
                // let _ = c.as_ref().n + 44 + 77;
                let _ = c.n + 44 + 77;
                // let _ = c.n + 44 + 77;
            }
            let end_timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap();
            println!("{:.2}", (end_timestamp - start_timestamp).as_millis());
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

struct BenchS {
    n: i32,
}

type TTT = i32;

struct TupleStruct;

#[derive(Debug)]
struct Val1 {
    v1: i32,
    v2: i32,
}

#[macro_export]
macro_rules! vec2 {
    ( $( $x:expr ),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push($x);
            )*
            temp_vec
        }
    };
}

pub trait ToSql: std::fmt::Debug {
    fn to_sql(&self);
}

impl ToSql for Vec<i32> {
    fn to_sql(&self) {
        println!("Vec<T>");
    }
}

enum IpAddrKind {
    V4,
    V6,
    TV4(i32, f64),
    TV6 { a: i32, b: f64 },
}

impl IpAddrKind {
    fn to_sql(&self) {
        println!("IpAddrKind");
        ()
    }
}

struct LinkedList2 {
    head: Option<Box<Node>>,
    // tail: Option<Box<Node>>,
}

struct Node {
    value: i32,
    next: Option<Box<Node>>,
}

impl LinkedList2 {
    fn new() -> Self {
        LinkedList2 { head: None }
    }

    fn push_left(&mut self, value: i32) {
        let o = Node {
            value,
            next: self.head.take(),
        };
        self.head = Some(Box::new(o));
    }

    fn push_right(&mut self, value: i32) {
        // TODO: am i right that it's impossible to implement this method with Box?
        // let mut last_ptr = self.head.as_mut();
        // while let Some(ref mut node) = last_ptr {
        //     last_ptr = node.next.as_mut();
        //     if last_ptr.unwrap().next.is_none() {
        //         break;
        //     }
        // }
        unimplemented!()
    }

    fn peek(&self) -> Vec<i32> {
        let mut out = vec![];
        let mut t = self.head.as_ref();
        while let Some(node) = t {
            out.push(node.value);
            t = node.next.as_ref();
        }
        out
    }

    fn pop_left(&mut self) {
        self.head = self.head.take().unwrap().next;
    }

    fn pop_right(&self) {
        unimplemented!()
    }
}

fn longest<'a>(x: &'a str, y: &'a str) -> &'a str {
    if x.len() > y.len() {
        x
    } else {
        y
    }
}

fn foo(x: &str) -> (&str, &str, &str) {
    (x, x, x)
}

struct S<'a, T> {
    start: *const T,
    end: *const T,
    phantom: std::marker::PhantomData<&'a T>,
}

fn execute_cpu_cache_optimization_benchmark() {
    let n = 10_000;
    let mut m_raw = vec![];
    for i in 0..n {
        let mut mm = vec![];
        for j in 0..n {
            mm.push(i + j);
        }
        m_raw.push(mm);
    }
    let m = m_raw.into_boxed_slice();
    let mut s = 0;
    let start_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    for i in 0..n {
        for j in 0..n {
            // NOTE: with m[i][j] cpu cache will be used, with m[j][i] don't
            //       venchmark: 1.15s vs vs 5.52s
            s += m[i][j];
        }
    }
    let end_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    println!("{:.2} s={s}", (end_timestamp - start_timestamp).as_millis());
}

///
/// https://betterprogramming.pub/learning-rust-building-a-linked-list-102bcb08f93b
///
mod article {
    pub struct LinkedListV1<T> {
        val: Option<T>,
        next: Option<Box<LinkedListV1<T>>>,
    }

    impl LinkedListV1<i32> {
        pub fn new() -> Self {
            LinkedListV1 {
                val: None,
                next: None,
            }
        }

        pub fn push_left(self, x: i32) -> LinkedListV1<i32> {
            LinkedListV1 {
                val: Some(x),
                next: Some(Box::new(self)),
            }
        }

        pub fn collect(&self) -> Vec<i32> {
            let mut p = self.next.as_ref();
            let mut out = vec![];
            if let Some(v) = self.val {
                out.push(v);
            }
            while let Some(p_in) = p {
                if p_in.val.is_none() {
                    break;
                }
                out.push(p_in.val.unwrap());
                p = p_in.next.as_ref();
            }
            out
        }
    }

    pub struct LinkedListV2<'a, T> {
        val: Option<T>,
        next: Option<&'a Box<LinkedListV2<'a, T>>>,
    }

    impl<'a> LinkedListV2<'a, i32> {
        pub fn new() -> Self {
            LinkedListV2 {
                val: None,
                next: None,
            }
        }

        pub fn push_left(&mut self, node: &'a mut Box<LinkedListV2<'a, i32>>) {
            match self.next {
                None => {
                    self.next = Some(node);
                }
                Some(_) => {
                    node.next = self.next;
                    self.next = Some(node);
                }
            }
        }

        pub fn create_box(x: i32) -> Box<LinkedListV2<'a, i32>> {
            Box::new(LinkedListV2 {
                val: Some(x),
                next: None,
            })
        }

        pub fn collect(&self) -> Vec<i32> {
            let mut out = vec![];
            let mut p = self.next;
            while let Some(v) = p {
                if p.is_none() {
                    break;
                }
                out.push(v.val.unwrap());
                p = v.next;
            }
            out
        }
    }

    pub struct LinkedListV3<T> {
        val: Option<T>,
        next: Option<std::ptr::NonNull<LinkedListV3<T>>>,
    }

    impl LinkedListV3<i32> {
        pub fn new() -> Self {
            LinkedListV3 {
                val: None,
                next: None,
            }
        }

        pub fn push_left(&mut self, x: i32) {
            let node = Box::new(LinkedListV3 {
                val: Some(x),
                next: None,
            });
            let mut ptr: std::ptr::NonNull<LinkedListV3<i32>> = Box::leak(node).into();
            if self.next.is_some() {
                unsafe {
                    ptr.as_mut().next = self.next;
                }
            }
            self.next = Some(ptr);
        }

        pub fn collect(&self) -> Vec<i32> {
            let mut out = vec![];
            let mut p = self.next;
            while let Some(v) = p {
                unsafe {
                    out.push(v.as_ref().val.unwrap());
                    p = v.as_ref().next;
                }
            }
            out
        }

        pub fn pop_left(&mut self) -> Option<i32> {
            unsafe {
                let node = self.next.unwrap().as_ref();
                self.next = node.next;
                node.val
            }
        }
    }
}

///
/// https://rust-unofficial.github.io/too-many-lists/first-final.html
///
mod article_2_a_bad_stack {
    pub struct List {
        head: Link,
    }

    pub enum Link {
        Empty,
        More(Box<Node>),
    }

    pub struct Node {
        elem: i32,
        next: Link,
    }

    impl List {
        pub fn new() -> Self {
            List { head: Link::Empty }
        }

        pub fn push(&mut self, elem: i32) {
            let node = Box::new(Node {
                elem,
                next: std::mem::replace(&mut self.head, Link::Empty),
            });
            self.head = Link::More(node);
        }

        pub fn collect(&self) -> Vec<i32> {
            let mut out = vec![];
            let mut p = &self.head;
            while let Link::More(node) = p {
                out.push(node.elem);
                p = &node.next;
            }
            out
        }
    }
}

mod article_2_an_ok_stack {
    pub struct List<T> {
        head: Link<T>,
    }

    type Link<T> = Option<Box<Node<T>>>;

    pub struct Node<T> {
        elem: T,
        next: Link<T>,
    }

    impl List<i32> {
        pub fn new() -> Self {
            List { head: None }
        }

        pub fn push(&mut self, elem: i32) {
            let node = Box::new(Node {
                elem,
                next: self.head.take(),
            });
            self.head = Some(node);
        }

        pub fn collect(&self) -> Vec<i32> {
            let mut out = vec![];
            let mut p = &self.head;
            while let Some(node) = p {
                out.push(node.elem);
                p = &node.next;
            }
            out
        }
    }
}

mod article_2_persistent_stack {
    pub struct List<T> {
        head: Link<T>,
    }

    type Link<T> = Option<std::rc::Rc<Node<T>>>;

    pub struct Node<T> {
        elem: T,
        next: Link<T>,
    }

    impl List<i32> {
        pub fn new() -> Self {
            List { head: None }
        }
        pub fn prepend(&self, x: i32) -> List<i32> {
            List {
                head: Some(std::rc::Rc::new(Node {
                    elem: x,
                    next: self.head.clone(),
                })),
            }
        }

        pub fn collect(&self) -> Vec<i32> {
            let mut out = vec![];
            let mut p = &self.head;
            while let Some(node) = p {
                out.push(node.elem);
                p = &node.next;
            }
            out
        }
    }
}

mod article_2_a_bad_safe_queue {
    use std::{cell::RefCell, rc::Rc};

    pub struct List<T> {
        head: Link<T>,
        tail: Link<T>,
    }

    type Link<T> = Option<Rc<RefCell<Node<T>>>>;

    pub struct Node<T> {
        elem: i32,
        next: Link<T>,
        prev: Link<T>,
    }

    impl Node<i32> {
        pub fn new(elem: i32) -> Rc<RefCell<Self>> {
            Rc::new(RefCell::new(Node {
                elem,
                next: None,
                prev: None,
            }))
        }
    }

    impl List<i32> {
        pub fn new() -> Self {
            List {
                head: None,
                tail: None,
            }
        }

        pub fn push_left(&mut self, elem: i32) {
            let new_head = Node::new(elem);
            match self.head.take() {
                None => {
                    self.head = Some(new_head.clone());
                    self.tail = Some(new_head);
                }
                Some(old_head) => {
                    old_head.borrow_mut().prev = Some(new_head.clone());
                    new_head.borrow_mut().next = Some(old_head);
                    self.head = Some(new_head);
                }
            }
        }

        pub fn collect(&self) -> Vec<i32> {
            let mut out = vec![];
            let mut p = self.head.clone();
            while let Some(node) = p {
                out.push(node.borrow().elem);
                p = node.borrow().next.clone();
            }
            out
        }
    }
}

mod article_2_an_ok_unsafe_queue {
    use std::ptr;

    pub struct List<T> {
        head: Link<T>,
        tail: Link<T>,
    }

    type Link<T> = *mut Node<T>;

    pub struct Node<T> {
        elem: T,
        next: Link<T>,
    }

    impl List<i32> {
        pub fn new() -> Self {
            List {
                head: ptr::null_mut(),
                tail: ptr::null_mut(),
            }
        }

        pub fn push_right(&mut self, x: i32) {
            unsafe {
                let new_node = Box::into_raw(Box::new(Node {
                    elem: x,
                    next: ptr::null_mut(),
                }));
                match self.tail.is_null() {
                    true => {
                        self.head = new_node;
                        self.tail = new_node;
                    }
                    false => {
                        (*self.tail).next = new_node;
                        self.tail = new_node;
                    }
                }
            }
        }

        pub fn collect(&self) -> Vec<i32> {
            unsafe {
                let mut out = vec![];
                let mut p = self.head;
                while !p.is_null() {
                    out.push((*p).elem);
                    p = (*p).next;
                }
                out
            }
        }
    }
}

mod stas_question {
    use std::cell::RefCell;

    #[derive(Debug)]
    pub struct A {
        pub val: i32,
        pub b: *const B,
    }

    #[derive(Debug)]
    pub struct B {
        pub val: i32,
        pub a: *const A,
    }
    use std::rc::Rc;

    #[derive(Debug)]
    pub struct AA {
        pub val: i32,
        pub b: Option<Rc<RefCell<BB>>>,
    }

    #[derive(Debug)]
    pub struct BB {
        pub val: i32,
        pub a: Option<Rc<RefCell<AA>>>,
    }
}
