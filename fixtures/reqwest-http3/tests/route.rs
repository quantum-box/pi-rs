use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

fn endpoint() -> quinn::Endpoint {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = rustls::pki_types::CertificateDer::from(
        include_bytes!("../../../crates/workbench/tests/fixtures/bedrock-proxy-tls/server.der")
            .to_vec(),
    );
    let key = rustls::pki_types::PrivateKeyDer::try_from(
        include_bytes!("../../../crates/workbench/tests/fixtures/bedrock-proxy-tls/server-key.der")
            .to_vec(),
    )
    .unwrap();
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap()
}

macro_rules! h3_security {
    ($name:ident, $client:ident) => {
        #[tokio::test]
        async fn $name() {
            let server = endpoint();
            let url = format!("https://localhost:{}/", server.local_addr().unwrap().port());
            let (finished, wait) = tokio::sync::oneshot::channel();
            let listener = tokio::spawn(async move {
                let incoming = timeout(Duration::from_secs(10), server.accept())
                    .await
                    .unwrap()
                    .unwrap();
                let connection = incoming.await.unwrap();
                let mut h3 = h3::server::Connection::<_, bytes::Bytes>::new(
                    h3_quinn::Connection::new(connection),
                )
                .await
                .unwrap();
                let resolver = h3.accept().await.unwrap().unwrap();
                let (request, mut stream) = resolver.resolve_request().await.unwrap();
                assert!(!request.headers().contains_key("proxy-authorization"));
                stream
                    .send_response(http::Response::builder().status(200).body(()).unwrap())
                    .await
                    .unwrap();
                stream.finish().await.unwrap();
                let _ = wait.await;
            });
            let client = $client::Client::builder()
                .no_proxy()
                .http3_prior_knowledge()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap();
            let response = timeout(
                Duration::from_secs(10),
                client
                    .get(&url)
                    .version(http::Version::HTTP_3)
                    .header("proxy-authorization", "Basic c2VjcmV0")
                    .send(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(response.status().is_success());
            let _ = finished.send(());
            listener.await.unwrap();
            let proxy = $client::Proxy::all("http://127.0.0.1:1")
                .unwrap()
                .basic_auth("user", "secret");
            let proxied = $client::Client::builder()
                .no_proxy()
                .proxy(proxy)
                .http3_prior_knowledge()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap();
            let err = timeout(
                Duration::from_secs(10),
                proxied.get(&url).version(http::Version::HTTP_3).send(),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert!(
                format!("{err:?}").contains("HTTP/3 proxy routing is unsupported"),
                "{err:?}"
            );
        }
    };
}
h3_security!(reqwest_012_direct_and_proxied, reqwest12);
h3_security!(reqwest_013_direct_and_proxied, reqwest13);
