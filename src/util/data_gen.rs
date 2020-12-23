pub fn get_empty_data(siz: usize) -> Vec<u8>{
    let mut vec = Vec::<u8>::with_capacity(siz);
    for _i in 0..siz {
        vec.push(0u8);
    }
    return vec;
}
