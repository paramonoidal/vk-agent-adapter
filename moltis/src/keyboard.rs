//! Persistent VK command keyboard, derived from the shared command registry.

use {
    moltis_channels::commands::{CommandDef, all_commands},
    serde_json::{Value, json},
    std::sync::OnceLock,
};

// https://dev.vk.com/ru/api/bots/development/keyboard
// Regular keyboards: 40 buttons total, 10 rows, at most 5 buttons per row.
const MAX_BUTTONS: usize = 40;
const BUTTONS_PER_ROW: usize = 4;
const PRIORITY: &[&str] = &[
    "/help",
    "/model all",
    "/node",
    "/usage",
    "/sh on",
    "/sh off",
    "/sh status",
    "/stop",
    "/new",
    "/sessions",
    "/fork",
    "/context",
    "/model",
    "/peek",
    "/approvals",
    "/insights",
];

pub(crate) fn command_keyboard() -> &'static str {
    static KEYBOARD: OnceLock<String> = OnceLock::new();
    KEYBOARD.get_or_init(|| build_keyboard(all_commands()).to_string())
}

fn build_keyboard(commands: &[CommandDef]) -> Value {
    let mut labels: Vec<String> = PRIORITY
        .iter()
        .filter(|label| {
            let name = label.trim_start_matches('/').split_whitespace().next();
            commands.iter().any(|cmd| Some(cmd.name) == name)
        })
        .take(MAX_BUTTONS)
        .map(|label| (*label).to_owned())
        .collect();
    for command in commands {
        if labels.len() == MAX_BUTTONS {
            break;
        }
        // /sh on represents the bare /sh action; all other commands also get
        // their bare invocation, even if a more useful preset appears first.
        let label = format!("/{}", command.name);
        if command.name != "sh" && !labels.contains(&label) {
            labels.push(label);
        }
    }
    let buttons: Vec<Vec<Value>> = labels
        .chunks(BUTTONS_PER_ROW)
        .map(|row| {
            row.iter()
                .map(|label| {
                    let color = match label.as_str() {
                        "/model all" | "/new" => "primary",
                        "/sh on" => "positive",
                        "/sh off" | "/stop" | "/clear" => "negative",
                        _ => "secondary",
                    };
                    // Text buttons send the exact slash command through the
                    // existing allowlist and command authorization checks.
                    json!({"action": {"type": "text", "label": label}, "color": color})
                })
                .collect()
        })
        .collect();
    json!({"one_time": false, "inline": false, "buttons": buttons})
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn labels(keyboard: &Value) -> Vec<&str> {
        keyboard["buttons"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|row| row.as_array().unwrap())
            .map(|button| button["action"]["label"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn command_keyboard_covers_registry_and_stays_within_vk_limits() {
        let keyboard: Value = serde_json::from_str(command_keyboard()).unwrap();
        let labels = labels(&keyboard);
        assert_eq!(keyboard["one_time"], false);
        assert_eq!(keyboard["inline"], false);
        assert!(labels.len() <= MAX_BUTTONS);
        assert!(keyboard["buttons"].as_array().unwrap().len() <= 10);
        assert!(keyboard["buttons"].as_array().unwrap().iter().all(|row| {
            let count = row.as_array().unwrap().len();
            (1..=5).contains(&count)
        }));
        assert!(labels.iter().all(|label| label.chars().count() <= 40));
        for cmd in all_commands() {
            assert!(
                labels.iter().any(|label| {
                    label.trim_start_matches('/').split_whitespace().next() == Some(cmd.name)
                }),
                "missing /{}",
                cmd.name
            );
        }
        assert_eq!(&labels[..4], &["/help", "/model all", "/node", "/usage"]);
        for label in ["/sh on", "/sh off", "/sh status"] {
            assert!(labels.contains(&label));
        }
    }

    #[test]
    fn oversized_registry_keeps_useful_commands_before_truncating() {
        let mut commands = all_commands().to_vec();
        for index in 0..80 {
            commands.push(CommandDef {
                name: Box::leak(format!("extra{index}").into_boxed_str()),
                description: "Extra command",
                arg: None,
            });
        }
        let keyboard = build_keyboard(&commands);
        let labels = labels(&keyboard);
        assert_eq!(labels.len(), MAX_BUTTONS);
        assert_eq!(keyboard["buttons"].as_array().unwrap().len(), 10);
        assert_eq!(&labels[..PRIORITY.len()], PRIORITY);
        assert_eq!(
            labels
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            MAX_BUTTONS
        );
    }
}
