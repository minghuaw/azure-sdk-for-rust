#![cfg(all(test, feature = "test_e2e"))]

#[allow(unused_macros)]
macro_rules! cfg_not_wasm32 {
    ($($item:item)*) => {
        $(
            #[cfg(not(target_arch = "wasm32"))]
            $item
        )*
    }
}

macro_rules! cfg_transaction {
    ($($item:item)*) => {
        $(
            #[cfg_attr(docsrs, doc(cfg(feature = "transaction")))]
            #[cfg(feature = "transaction")]
            $item
        )*
    }
}
