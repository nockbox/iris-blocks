use crate::rt::{RtBound, RtSync};
use iris_grpc_proto::pb::private::v1::{
    nock_app_service_client::NockAppServiceClient, peek_response::Result as PeekResult, *,
};
use iris_ztd::{cue, jam, NounDecode, NounEncode};
use thiserror::Error;
use tonic::codegen::{Body, Bytes, StdError};

#[derive(Debug, Error)]
pub enum NounError {
    #[error("Unable to cue noun")]
    Cue,
    #[error("Unable to decode noun")]
    Decode,
}

#[derive(Debug, Error)]
#[error("Scry failed")]
pub struct ScryFailed;

#[derive(Debug, Error)]
pub enum ScryError {
    #[error(transparent)]
    Noun(#[from] NounError),
    #[error(transparent)]
    Tonic(#[from] tonic::Status),
    #[error(transparent)]
    ScryFailed(ScryFailed),
}

pub trait Scryable: Clone + RtBound + RtSync + 'static {
    fn remote_scry<'a, T: NounDecode + RtBound + 'a>(
        &'a mut self,
        path: impl NounEncode + RtBound + 'a,
    ) -> impl core::future::Future<Output = Result<T, ScryError>> + RtBound + 'a;
}

impl<C: tonic::client::GrpcService<tonic::body::BoxBody> + RtBound + RtSync + Clone + 'static>
    Scryable for NockAppServiceClient<C>
where
    C::Error: Into<StdError>,
    C::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
    <C::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    <C as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<tonic::codegen::Bytes, tonic::Status>,
    >>::Future: RtBound,
{
    async fn remote_scry<'a, T: NounDecode + RtBound + 'a>(
        &'a mut self,
        path: impl NounEncode + RtBound + 'a,
    ) -> Result<T, ScryError>
/*where
        C::Error: Into<StdError>,
        C::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <C::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,*/ {
        let peek_req = PeekRequest {
            pid: 0,
            path: jam(path.to_noun()),
        };

        let peek_res = self.peek(peek_req).await?.into_inner();
        let Some(PeekResult::Data(peek_blob)) = peek_res.result else {
            return Err(ScryError::ScryFailed(ScryFailed));
        };
        let peek_noun = cue(&peek_blob).ok_or(ScryError::Noun(NounError::Cue))?;
        let v = NounDecode::from_noun(&peek_noun).ok_or(ScryError::Noun(NounError::Decode))?;
        Ok(v)
    }
}
