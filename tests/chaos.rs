use std::collections::HashMap;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

struct Node {
    id: u16,
    child: Child,
}

impl Node {
    fn start(id: u16) -> Self {
        let child = Command::new("cargo")
            .args(["run", "--release", "--quiet", "--", &id.to_string()])
            .spawn()
            .expect("failed to start node process");

        // Give the node time to bind sockets and start servers.
        thread::sleep(Duration::from_millis(1200));

        Self { id, child }
    }

    fn port(&self) -> u16 {
        3000 + self.id
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn put(port: u16, key: &str, val: &str) -> u16 {
    let url = format!("http://127.0.0.1:{}/v1/keys/{}", port, key);
    let body = format!(r#"{{"value":"{}"}}"#, val);

    let out = Command::new("curl")
        .args([
            "-s",
            "-m",
            "2",
            "-X",
            "PUT",
            "-H",
            "Content-Type: application/json",
            "-d",
            &body,
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &url,
        ])
        .output();

    match out {
        Ok(o) => String::from_utf8_lossy(&o.stdout)
            .trim()
            .parse::<u16>()
            .unwrap_or(0),
        Err(_) => 0,
    }
}

fn get_value(port: u16, key: &str) -> Option<String> {
    let url = format!("http://127.0.0.1:{}/v1/keys/{}", port, key);
    let out = Command::new("curl")
        .args(["-s", "-m", "2", "-w", "\n%{http_code}", &url])
        .output()
        .ok()?;

    let text = String::from_utf8(out.stdout).ok()?;
    let (body, status_line) = text.rsplit_once('\n')?;
    let status = status_line.trim().parse::<u16>().ok()?;
    if status != 200 {
        return None;
    }

    let json: serde_json::Value = serde_json::from_str(body).ok()?;
    json.get("value")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn put_to_any_node(nodes: &[Node], key: &str, val: &str) -> bool {
    for n in nodes {
        if put(n.port(), key, val) == 200 {
            return true;
        }
    }
    false
}

#[test]
#[ignore = "chaos test: starts real processes and may take ~30-60s"]
fn chaos_no_acknowledged_write_loss() {
    let mut nodes = vec![
        Node::start(1),
        Node::start(2),
        Node::start(3),
        Node::start(4),
        Node::start(5),
    ];

    let mut confirmed: HashMap<String, String> = HashMap::new();

    for i in 0..120 {
        // Periodically kill and restart a node to inject churn.
        if i > 0 && i % 25 == 0 {
            let victim = (i / 25) as usize % nodes.len();
            let id = nodes[victim].id;
            println!("chaos: restarting node {}", id);
            nodes[victim].kill();
            nodes[victim] = Node::start(id);
        }

        let key = format!("key-{}", i);
        let val = format!("val-{}", i);

        // Retry writes while leadership is stabilizing.
        let mut ok = false;
        for _ in 0..8 {
            if put_to_any_node(&nodes, &key, &val) {
                ok = true;
                break;
            }
            thread::sleep(Duration::from_millis(150));
        }

        if ok {
            confirmed.insert(key, val);
        }
    }

    println!("confirmed writes: {}", confirmed.len());
    assert!(
        !confirmed.is_empty(),
        "test did not confirm any writes; cluster likely did not stabilize"
    );

    // Let final replication settle before verification.
    thread::sleep(Duration::from_millis(1200));

    let mut lost = 0usize;
    for (k, expected) in &confirmed {
        let mut found = false;
        for n in &nodes {
            if let Some(got) = get_value(n.port(), k) {
                if &got == expected {
                    found = true;
                    break;
                }
            }
        }
        if !found {
            lost += 1;
        }
    }

    println!("lost writes: {}", lost);
    assert_eq!(lost, 0, "DATA LOSS DETECTED");
}

#[test]
#[ignore = "chaos test: starts real processes and may take ~45-90s"]
fn chaos_leader_disruption_during_heavy_writes() {
    let mut nodes = vec![
        Node::start(1),
        Node::start(2),
        Node::start(3),
        Node::start(4),
        Node::start(5),
    ];

    let mut confirmed: HashMap<String, String> = HashMap::new();

    for i in 0..180 {
        // Repeatedly disrupt node 1 (likely leader early in cluster lifetime).
        if i > 0 && i % 30 == 0 {
            println!("chaos: forcing node 1 restart at op {}", i);
            nodes[0].kill();
            thread::sleep(Duration::from_millis(500));
            nodes[0] = Node::start(1);
        }

        let key = format!("hot-key-{}", i);
        let val = format!("hot-val-{}", i);

        // Try node 1 first (hot path), then fan out to all nodes if needed.
        let mut ok = put(nodes[0].port(), &key, &val) == 200;
        if !ok {
            ok = put_to_any_node(&nodes, &key, &val);
        }

        if ok {
            confirmed.insert(key, val);
        } else {
            // Brief backoff to let a new leader stabilize.
            thread::sleep(Duration::from_millis(120));
        }
    }

    println!(
        "confirmed writes after leader disruption: {}",
        confirmed.len()
    );
    assert!(
        !confirmed.is_empty(),
        "test did not confirm any writes; cluster likely failed to stabilize"
    );

    thread::sleep(Duration::from_millis(1500));

    let mut lost = 0usize;
    for (k, expected) in &confirmed {
        let mut found = false;
        for n in &nodes {
            if let Some(got) = get_value(n.port(), k) {
                if &got == expected {
                    found = true;
                    break;
                }
            }
        }
        if !found {
            lost += 1;
        }
    }

    println!("lost after leader disruption: {}", lost);
    assert_eq!(lost, 0, "DATA LOSS DETECTED UNDER LEADER DISRUPTION");
}
