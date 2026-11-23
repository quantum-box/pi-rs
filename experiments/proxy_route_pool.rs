//! Isolated design experiment, not production transport code.
//! Run: rustc --edition=2021 --test experiments/proxy_route_pool.rs -o /tmp/pi-route-pool && /tmp/pi-route-pool
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

// No Debug: this prototype holds secret bytes in memory and is not suitable
// for production until retention and zeroization have been addressed.
#[derive(Clone, Hash, PartialEq, Eq)]
enum Route {
    Direct,
    Proxy {
        authority: String,
        tls: bool,
        tunnel: bool,
        authorization: Vec<u8>,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    },
}

struct Client {
    id: usize,
}
struct State {
    clients: HashMap<Route, Arc<Client>>,
    lru: VecDeque<Route>,
    next_id: usize,
}
struct RoutePool {
    state: Mutex<State>,
    capacity: usize,
}
impl RoutePool {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            state: Mutex::new(State {
                clients: HashMap::new(),
                lru: VecDeque::new(),
                next_id: 0,
            }),
            capacity,
        }
    }
    fn client(&self, route: Route) -> Arc<Client> {
        let mut state = self.state.lock().unwrap();
        let client = if let Some(client) = state.clients.get(&route) {
            client.clone()
        } else {
            let client = Arc::new(Client { id: state.next_id });
            state.next_id += 1;
            state.clients.insert(route.clone(), client.clone());
            client
        };
        state.lru.retain(|entry| entry != &route);
        state.lru.push_back(route);
        while state.lru.len() > self.capacity {
            let evicted = state.lru.pop_front().unwrap();
            state.clients.remove(&evicted);
        }
        client
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn proxy(secret: &[u8], tunnel: bool) -> Route {
        Route::Proxy {
            authority: "proxy.example:8080".into(),
            tls: false,
            tunnel,
            authorization: secret.into(),
            headers: vec![],
        }
    }

    #[test]
    fn isolates_credentials_and_transport_but_reuses_same_route() {
        let pool = RoutePool::new(8);
        let a = pool.client(proxy(b"a", false));
        assert_eq!(a.id, pool.client(proxy(b"a", false)).id);
        assert_ne!(a.id, pool.client(proxy(b"b", false)).id);
        assert_ne!(a.id, pool.client(proxy(b"a", true)).id);
        assert_ne!(a.id, pool.client(Route::Direct).id);
    }

    #[test]
    fn concurrent_hops_choose_separate_routes_and_share_only_identical_routes() {
        let pool = Arc::new(RoutePool::new(8));
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let threads: Vec<_> = (0..16)
            .map(|index| {
                let pool = pool.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let first = pool.client(proxy(if index % 2 == 0 { b"a" } else { b"b" }, false));
                    let redirect = pool.client(Route::Direct);
                    (index % 2, first.id, redirect.id)
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        let even = results.iter().find(|r| r.0 == 0).unwrap().1;
        let odd = results.iter().find(|r| r.0 == 1).unwrap().1;
        assert_ne!(even, odd);
        assert!(results
            .iter()
            .all(|r| r.1 == if r.0 == 0 { even } else { odd }));
        assert!(results.iter().all(|r| r.2 == results[0].2));
    }

    #[test]
    fn active_client_survives_eviction_and_recreated_route_gets_new_pool() {
        let pool = RoutePool::new(1);
        let active = pool.client(proxy(b"a", false));
        pool.client(Route::Direct);
        let replacement = pool.client(proxy(b"a", false));
        assert_ne!(active.id, replacement.id);
        assert_eq!(active.id, 0);
    }
}
