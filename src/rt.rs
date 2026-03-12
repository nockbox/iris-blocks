use core::future::Future;
pub use imp::*;

#[cfg(feature = "tokio")]
mod imp {
    use super::*;

    pub async fn sleep(duration: std::time::Duration) {
        tokio::time::sleep(duration).await;
    }

    pub fn spawn<F: core::future::Future + RtBound + 'static>(f: F)
    where
        F::Output: Send,
    {
        tokio::spawn(f);
    }

    pub async fn spawn_blocking<R: RtBound + 'static, T: FnOnce() -> R + RtBound + 'static>(
        f: T,
    ) -> R {
        tokio::task::spawn_blocking(f).await.unwrap()
    }

    pub trait RtBound: Send {}
    impl<T: ?Sized + Send> RtBound for T {}

    pub type RtFuture<'a, T> = dyn Future<Output = T> + Send + 'a;
}

#[cfg(feature = "wasm")]
mod imp {
    use super::*;
    use wasm_bindgen::convert::IntoWasmAbi;
    use wasm_bindgen_futures::*;

    pub async fn sleep(duration: std::time::Duration) {
        let js = format!(
            "new Promise(resolve => setTimeout(resolve, {}))",
            duration.as_millis()
        );
        let promise = js_sys::eval(&js).expect("js sleep eval failed");
        wasm_bindgen_futures::JsFuture::from(js_sys::Promise::from(promise))
            .await
            .expect("js sleep promise failed");
    }

    pub fn spawn<F: core::future::Future + RtBound + 'static>(f: F) {
        spawn_local(async move {
            f.await;
        });
    }

    pub async fn spawn_blocking<R, T: FnOnce() -> R + RtBound + 'static>(f: T) -> R {
        f()
    }

    pub trait RtBound {}
    impl<T: ?Sized> RtBound for T {}

    pub type RtFuture<'a, T> = dyn Future<Output = T> + 'a;
}
