use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Cell, Paragraph, Row, Table, Wrap};
use tokio::sync::mpsc;

use crate::health::{HealthRegistry, ListenerStatus, ServiceSnapshot, TargetStatus};

const TICK_RATE: Duration = Duration::from_millis(150);

const INK: Color = Color::Rgb(226, 232, 240);
const MUTED: Color = Color::Rgb(100, 116, 139);
const PANEL: Color = Color::Rgb(15, 23, 42);
const BORDER: Color = Color::Rgb(51, 65, 85);
const ACCENT: Color = Color::Rgb(56, 189, 248);
const SUCCESS: Color = Color::Rgb(74, 222, 128);
const WARNING: Color = Color::Rgb(251, 191, 36);
const INFO: Color = Color::Rgb(129, 140, 248);

/// Run the full-screen dashboard on the current thread. This is blocking and
/// is intended to be called from `tokio::task::spawn_blocking`.
pub fn run(
    health: HealthRegistry,
    shutdown_tx: mpsc::UnboundedSender<()>,
    stop: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let mut terminal = ratatui::try_init()?;
    let result = run_loop(&mut terminal, health, shutdown_tx, stop);
    let restore = ratatui::try_restore();

    result?;
    restore?;
    Ok(())
}

fn run_loop(
    terminal: &mut ratatui::DefaultTerminal,
    health: HealthRegistry,
    shutdown_tx: mpsc::UnboundedSender<()>,
    stop: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let mut shutdown_requested = false;

    while !stop.load(Ordering::Acquire) {
        let snapshots = health.snapshots();
        terminal.draw(|frame| render(frame, &snapshots, shutdown_requested))?;

        if event::poll(TICK_RATE)? {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if is_shutdown_key(key) && !shutdown_requested {
                shutdown_requested = true;
                let _ = shutdown_tx.send(());
            }
        }
    }

    Ok(())
}

fn is_shutdown_key(key: KeyEvent) -> bool {
    key.kind == KeyEventKind::Press
        && (key.code == KeyCode::Char('q')
            || key.code == KeyCode::Esc
            || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL)))
}

fn render(frame: &mut Frame, services: &[ServiceSnapshot], shutdown_requested: bool) {
    let area = frame.area();
    if area.width < 68 || area.height < 12 {
        render_too_small(frame, area);
        return;
    }

    let [header, table, footer] = Layout::vertical([
        Constraint::Length(5),
        Constraint::Min(5),
        Constraint::Length(2),
    ])
    .areas(area);

    render_header(frame, header, services);
    render_services(frame, table, services);
    render_footer(frame, footer, shutdown_requested);
}

fn render_header(frame: &mut Frame, area: Rect, services: &[ServiceSnapshot]) {
    let [brand, summary] =
        Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)]).areas(area);

    let brand_widget = Paragraph::new(vec![
        Line::from(Span::styled(
            "  tunl",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  service health, without the log hunt",
            Style::default().fg(MUTED),
        )),
    ])
    .block(Block::default().padding(ratatui::widgets::Padding::new(0, 0, 1, 0)));
    frame.render_widget(brand_widget, brand);

    let reachable = services
        .iter()
        .filter(|service| service.target_status == TargetStatus::Reachable)
        .count();
    let unreachable = services
        .iter()
        .filter(|service| service.target_status == TargetStatus::Unreachable)
        .count();
    let connections: usize = services
        .iter()
        .map(|service| service.active_connections)
        .sum();

    let summary_line = Line::from(vec![
        Span::styled(
            format!("{reachable} reachable"),
            Style::default().fg(SUCCESS),
        ),
        Span::styled("   ", Style::default()),
        Span::styled(
            format!("{unreachable} unreachable"),
            Style::default().fg(if unreachable == 0 { MUTED } else { WARNING }),
        ),
        Span::styled("   ", Style::default()),
        Span::styled(format!("{connections} active"), Style::default().fg(INFO)),
    ]);
    let summary_widget = Paragraph::new(summary_line)
        .alignment(Alignment::Right)
        .block(Block::default().padding(ratatui::widgets::Padding::new(0, 2, 2, 0)));
    frame.render_widget(summary_widget, summary);
}

fn render_services(frame: &mut Frame, area: Rect, services: &[ServiceSnapshot]) {
    let panel = Block::default()
        .title(Line::from(Span::styled(
            format!(" Services  {} ", services.len()),
            Style::default().fg(INK).add_modifier(Modifier::BOLD),
        )))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(PANEL))
        .padding(ratatui::widgets::Padding::horizontal(1));

    let inner = panel.inner(area);
    frame.render_widget(panel, area);

    if services.is_empty() {
        let empty = Paragraph::new(Line::from(vec![
            Span::styled("No services running", Style::default().fg(INK)),
            Span::styled("  Waiting for configuration…", Style::default().fg(MUTED)),
        ]))
        .alignment(Alignment::Center);
        frame.render_widget(empty, inner);
        return;
    }

    let has_errors = services.iter().any(|service| service.last_error.is_some());
    let show_errors = has_errors && inner.height >= 8;
    let [table_area, errors_area] = if show_errors {
        let error_rows = services
            .iter()
            .filter(|service| service.last_error.is_some())
            .count();
        let error_height = ((error_rows * 2 + 1) as u16).min(8).min(inner.height / 2);
        Layout::vertical([Constraint::Min(4), Constraint::Length(error_height)]).areas(inner)
    } else {
        [inner, Rect::default()]
    };

    let header = Row::new([
        "SERVICE",
        "LOCAL",
        "TARGET",
        "LISTENER",
        "REACHABILITY",
        "ACTIVE",
        "ACTIVITY",
    ])
    .style(Style::default().fg(MUTED).add_modifier(Modifier::BOLD))
    .height(2);

    let rows = services.iter().map(|service| {
        let (listener_label, listener_color) = listener_style(service.listener_status);
        let listener = Line::from(vec![
            Span::styled("● ", Style::default().fg(listener_color)),
            Span::styled(listener_label, Style::default().fg(listener_color)),
        ]);
        let (target_label, target_color) = target_style(service.target_status);
        let target_status = Line::from(vec![
            Span::styled("● ", Style::default().fg(target_color)),
            Span::styled(target_label, Style::default().fg(target_color)),
        ]);

        Row::new([
            Cell::from(service.name.clone())
                .style(Style::default().fg(INK).add_modifier(Modifier::BOLD)),
            Cell::from(service.local_address.to_string()).style(Style::default().fg(INK)),
            Cell::from(service.target.clone()).style(Style::default().fg(INK)),
            Cell::from(listener),
            Cell::from(target_status),
            Cell::from(service.active_connections.to_string()).style(Style::default().fg(INFO)),
            Cell::from(format_age(service.target_status_age)).style(Style::default().fg(MUTED)),
        ])
        .height(2)
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(16),
            Constraint::Length(22),
            Constraint::Min(24),
            Constraint::Length(12),
            Constraint::Length(14),
            Constraint::Length(8),
            Constraint::Length(9),
        ],
    )
    .header(header)
    .column_spacing(2);

    frame.render_widget(table, table_area);

    if show_errors {
        render_errors(frame, errors_area, services);
    }
}

fn render_errors(frame: &mut Frame, area: Rect, services: &[ServiceSnapshot]) {
    let mut lines = vec![Line::from(Span::styled(
        "Latest probe errors",
        Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
    ))];

    for service in services {
        let Some(error) = service.last_error.as_deref() else {
            continue;
        };
        lines.push(Line::from(vec![
            Span::styled(service.name.clone(), Style::default().fg(INK)),
            Span::styled("  ", Style::default()),
            Span::styled(service.target.clone(), Style::default().fg(MUTED)),
        ]));
        lines.push(Line::from(Span::styled(
            error,
            Style::default().fg(WARNING),
        )));
    }

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn render_footer(frame: &mut Frame, area: Rect, shutdown_requested: bool) {
    let content = if shutdown_requested {
        Line::from(vec![
            Span::styled("● ", Style::default().fg(WARNING)),
            Span::styled(
                "Shutting down — draining active connections",
                Style::default().fg(INK),
            ),
        ])
    } else {
        Line::from(vec![
            Span::styled(" live ", Style::default().bg(SUCCESS).fg(Color::Black)),
            Span::styled(
                "   q",
                Style::default().fg(INK).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                " quit   •   updates automatically",
                Style::default().fg(MUTED),
            ),
        ])
    };
    frame.render_widget(Paragraph::new(content).alignment(Alignment::Right), area);
}

fn render_too_small(frame: &mut Frame, area: Rect) {
    let message = Paragraph::new(vec![
        Line::from(Span::styled(
            "tunl",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "Make this window at least 68 × 12",
            Style::default().fg(MUTED),
        )),
    ])
    .alignment(Alignment::Center);
    frame.render_widget(message, area);
}

fn listener_style(status: ListenerStatus) -> (&'static str, Color) {
    match status {
        ListenerStatus::Listening => ("Listening", SUCCESS),
        ListenerStatus::Draining => ("Draining", MUTED),
    }
}

fn target_style(status: TargetStatus) -> (&'static str, Color) {
    match status {
        TargetStatus::Unknown => ("Unknown", MUTED),
        TargetStatus::Probing => ("Probing", INFO),
        TargetStatus::Reachable => ("Reachable", SUCCESS),
        TargetStatus::Unreachable => ("Unreachable", WARNING),
    }
}

fn format_age(age: Duration) -> String {
    if age.as_secs() < 2 {
        "now".to_string()
    } else if age.as_secs() < 60 {
        format!("{}s", age.as_secs())
    } else if age.as_secs() < 3_600 {
        format!("{}m", age.as_secs() / 60)
    } else {
        format!("{}h", age.as_secs() / 3_600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn dashboard_renders_service_details() {
        let health = HealthRegistry::default();
        health.register(
            "api".to_string(),
            "127.0.0.1:8080".parse().unwrap(),
            "remote://api.internal:80".to_string(),
        );
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).unwrap();
        let snapshots = health.snapshots();

        terminal
            .draw(|frame| render(frame, &snapshots, false))
            .unwrap();

        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("tunl"));
        assert!(rendered.contains("api"));
        assert!(rendered.contains("127.0.0.1:8080"));
        assert!(rendered.contains("Listening"));
    }

    #[test]
    fn dashboard_renders_long_probe_errors_in_full_width_section() {
        let health = HealthRegistry::default();
        let service = health.register(
            "demo".to_string(),
            "[::1]:9000".parse().unwrap(),
            "docker://tunl-demo:8000".to_string(),
        );
        service.mark_target_unreachable(&anyhow::anyhow!(
            "cannot reach the Docker daemon - is it running? (on macOS, start Docker Desktop)"
        ));

        let mut terminal = Terminal::new(TestBackend::new(150, 24)).unwrap();
        let snapshots = health.snapshots();

        terminal
            .draw(|frame| render(frame, &snapshots, false))
            .unwrap();

        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("Latest probe errors"));
        assert!(rendered.contains("cannot reach the Docker daemon"));
        assert!(rendered.contains("start Docker Desktop"));
    }
}
