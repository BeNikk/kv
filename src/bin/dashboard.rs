use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use kv::dashboard::NodeStatus;
use ratatui::{prelude::*, widgets::*};
use std::{collections::HashMap, io, time::Duration};

#[tokio::main]
async fn main() -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = ratatui::Terminal::new(backend)?;

    let sock = tokio::net::UdpSocket::bind("0.0.0.0:9000").await?;
    let mut buf = [0u8; 4096];
    let mut statuses: HashMap<u64, NodeStatus> = HashMap::new();

    loop {
        if let Ok((len, _)) = sock.try_recv_from(&mut buf) {
            if let Ok(s) = serde_json::from_slice::<NodeStatus>(&buf[..len]) {
                statuses.insert(s.id, s);
            }
        }

        terminal.draw(|f| render(&statuses, f))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(k) = event::read()? {
                if k.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}

fn render(statuses: &HashMap<u64, NodeStatus>, frame: &mut Frame) {
    let mut nodes: Vec<&NodeStatus> = statuses.values().collect();
    nodes.sort_by_key(|s| s.id);

    let rows: Vec<Row> = nodes
        .iter()
        .map(|s| {
            let style = match s.role.as_str() {
                "Leader" => Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
                "Candidate" => Style::default().fg(Color::Yellow),
                _ if !s.alive => Style::default().fg(Color::Red),
                _ => Style::default().fg(Color::White),
            };

            Row::new(vec![
                format!("Node {}", s.id),
                s.role.clone(),
                s.term.to_string(),
                s.log_length.to_string(),
                s.commit_index.to_string(),
                s.last_applied.to_string(),
                if s.alive {
                    "alive".to_string()
                } else {
                    "dead".to_string()
                },
            ])
            .style(style)
        })
        .collect();

    let widths = [
        Constraint::Length(8),
        Constraint::Length(12),
        Constraint::Length(6),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(8),
    ];

    let table = Table::new(rows, widths)
        .header(
            Row::new(["Node", "Role", "Term", "Log", "Commit", "Applied", "Status"])
                .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(
            Block::default()
                .title(" Raft Cluster - press q to quit ")
                .borders(Borders::ALL),
        );

    frame.render_widget(table, frame.size());
}
