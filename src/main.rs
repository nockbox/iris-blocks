//use iris_blocks::IrisPeekProxy;
use clap::Parser;
use core::net::SocketAddr;
use iris_blocks::chain_activations::ChainActivations;
use iris_blocks::layers::{
    l0::{L0Client, L0Config},
    l1::L1Client,
    layer::LayerDependency,
};
use iris_grpc_proto::pb::private::v1::nock_app_service_client::NockAppServiceClient;
use std::sync::Arc;
use tonic::transport::{Channel, Uri};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser, Debug)]
#[command(name = "iris-peek-proxy", about = "Iris peek proxy", long_about = None)]
pub struct Args {
    #[arg(short, long, default_value = "[::1]:50051")]
    pub bind: SocketAddr,
    #[arg(short, long)]
    pub connect: Option<Uri>,
    #[arg(short, long, default_value = "nockchain.sqlite")]
    pub db: String,
    #[arg(short, long, default_value = "false")]
    pub run_migrations: bool,
    #[command(flatten)]
    pub l0: L0Config,
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::EnvFilter::from_default_env();

    let sub = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(true)
            .with_target(true)
            .with_level(true),
    );

    #[cfg(feature = "tracy")]
    if std::env::var("TRACY_DISABLE").is_err() {
        let tracy = tracing_tracy::TracyLayer::default();
        sub.with(filter).with(tracy).init();
    } else {
        sub.with(filter).init();
    }
    #[cfg(not(feature = "tracy"))]
    sub.with(filter).init();

    let args = Args::parse();
    let _addr = args.bind;

    let scry = if let Some(connect) = args.connect {
        Some(NockAppServiceClient::new(
            Channel::builder(connect).connect().await?,
        ))
    } else {
        None
    };

    let mut conn = iris_blocks::db::new_conn(&args.db, 1).await?;

    if args.run_migrations {
        iris_blocks::db::run_migrations(&mut conn).await;
    }

    let activations = ChainActivations::mainnet();
    let l1_client = Arc::new(L1Client::new(activations.clone(), vec![]));
    let l0_deps: Vec<Arc<dyn LayerDependency>> = vec![l1_client.clone()];

    let client = L0Client::new(conn, scry, args.l0, activations.clone(), l0_deps);
    client.run().await;

    /*let proxy = IrisPeekProxy::new(chan);

    Server::builder()
       .accept_http1(true)
       // This will apply the gRPC-Web translation layer
       .layer(GrpcWebLayer::new())
       .add_service(NockAppServiceServer::new(proxy))
       .serve(addr)
       .await?;*/

    Ok(())
}
