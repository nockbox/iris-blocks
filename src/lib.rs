use iris_ztd::{Digest, Noun, NounDecode, NounEncode};

pub mod chain_activations;
pub mod db;
pub mod layers;
mod rt;
pub mod scry;

#[cfg(feature = "wasm")]
pub mod wasm;

#[derive(Clone, Copy)]
pub struct StringDigest(Digest);

impl core::fmt::Display for StringDigest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl core::fmt::Debug for StringDigest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl NounEncode for StringDigest {
    fn to_noun(&self) -> Noun {
        self.0.to_string().to_noun()
    }
}

impl NounDecode for StringDigest {
    fn from_noun(noun: &Noun) -> Option<Self> {
        let s: String = NounDecode::from_noun(noun)?;
        Some(Self(Digest::try_from(&*s).ok()?))
    }
}

/*#[derive(Clone)]
pub struct IrisPeekProxy {
    client: NockAppServiceClient<Channel>,
}

impl IrisPeekProxy {
    pub fn new(channel: Channel) -> Self {
        Self { client: NockAppServiceClient::new(channel) }
    }
}


#[derive(Debug, Clone, Copy, iris_ztd::NounEncode, iris_ztd::NounDecode)]
pub enum Scry {
    #[noun_tag("mainnet")]
    Mainnet(()),
    #[noun_tag("block")]
    Block(StringDigest, ()),
    #[noun_tag("block-transactions")]
    BlockTransactions(StringDigest, ()),
    #[noun_tag("block-transaction")]
    BlockTransaction(StringDigest, StringDigest, ()),
    #[noun_tag("raw-transaction")]
    RawTransaction(StringDigest, ()),
    #[noun_tag("balance")]
    Balance(StringDigest, ()),
    #[noun_tag("tx-accepted")]
    TxAccepted {
        tx_id: StringDigest,
        sig: (),
    },
}

struct CacheEntry {
    depends_on_heavy: Option<StringDigest>,
    jam: Vec<u8>,
}

struct HeavyBoundCache {
    heavy: StringDigest,
    cache: lru::LruCache<Digest, Vec<u8>>,
}

pub struct LruCache {
    heavy_cache: HeavyBoundCache,
    reftr_cache: lru::LruCache<Digest, Vec<u8>>,
}

impl LruCache {
    pub async fn cached_peek(&mut self, peek_hash: Digest, depends_on_heavy: bool, client: &mut NockAppServiceClient<Channel>, raw_peek: PeekRequest) -> Result<PeekResponse, tonic::Status> {
        if depends_on_heavy {
            // Loop so that cache entry validity is dependent on us checking heavy last
            // Try 64 times max. Otherwise, return an error
            for _ in 0..64 {
                let heavy_peek = jam(["heavy", ()].to_noun());
                let cur_heavy = client.peek(PeekRequest { path: heavy_peek }).await?;
                let cur_heavy = cue(&cur_heavy.jam).ok_or_else(|| tonic::Status::internal("Invalid heavy"))?;
                let Some(Some(Some(Some(cur_heavy)))): Option<Option<Option<Option<Digest>>>> = NounDecode::from_noun(&cur_heavy) else {
                    return Err(tonic::Status::internal("Invalid heavy"));
                };

                if self.heavy_cache.heavy != cur_heavy {
                    self.heavy_cache.heavy = cur_heavy;
                    self.heavy_cache.cache.clear();
                }

                if let Some(entry) = self.heavy_cache.cache.get(&peek_hash) {
                    return Ok(PeekResponse { jam: entry.clone() });
                }

                let response = client.peek(raw_peek).await?.into_inner();
                self.heavy_cache.cache.insert(peek_hash, response.jam.clone());
                // no break
            }
            Err(tonic::Status::internal("Node keeps changing heavy"))
        } else {
            if let Some(entry) = self.reftr_cache.get(&peek_hash) {
                return Ok(PeekResponse { jam: entry.clone() });
            }

            let response = client.peek(raw_peek).await?.into_inner();
            self.reftr_cache.insert(peek_hash, response.jam.clone());
            Ok(response)
        }
    }
}

#[tonic::async_trait]
impl NockAppService for IrisPeekProxy {
    async fn peek(&self, request: tonic::Request<PeekRequest>) -> Result<tonic::Response<PeekResponse>, tonic::Status> {
        let request = request.into_inner();

        let path = cue(&request.path).ok_or_else(|| tonic::Status::invalid_argument("Path invalid noun"))?;
        let scry: Scry = NounDecode::from_noun(&path).ok_or_else(|| tonic::Status::invalid_argument("Unsupported path"))?;

        println!("Request: {:?}", scry);

        let mut client = self.client.clone();
        let response = client.peek(request).await?;
        Ok(response)
    }

    async fn poke(&self, _: tonic::Request<PokeRequest>) -> Result<tonic::Response<PokeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented"))
    }
}*/
