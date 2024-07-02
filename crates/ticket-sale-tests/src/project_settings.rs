use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

use eyre::{eyre, Result};
use serde::Deserialize;

#[derive(Clone, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct ProjectSettings {
    #[serde(skip)]
    pub project_root: PathBuf,

    #[allow(unused)]
    pub team_name: String,

    pub language: ProgrammingLanguage,

    #[serde(default)]
    pub bonus_implemented: bool,
    #[serde(default)]
    pub test_bonus: bool,

    #[serde(default)]
    pub java: JavaSettings,
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum ProgrammingLanguage {
    Rust,
    Java,
}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct JavaSettings {
    #[serde(default)]
    pub jar: PathBuf,
}

impl ProjectSettings {
    pub fn load() -> Result<Self> {
        let mut path = std::env::current_dir()?;
        let contents = loop {
            path.push("project.toml");

            match std::fs::read_to_string(&path) {
                Ok(s) => break s,
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }

            path.pop();
            if !path.pop() {
                return Err(eyre!("Could not find project.toml"));
            }
        };
        path.pop();

        let mut settings: ProjectSettings = toml::from_str(&contents)?;
        settings.project_root = path;

        if settings.java.jar.as_os_str().is_empty() {
            settings.java.jar = Path::join(&settings.project_root, "out/cli.jar");
        } else if settings.java.jar.is_relative() {
            settings.java.jar = Path::join(&settings.project_root, settings.java.jar);
        }

        if let Some(v) = std::env::var_os("CP_LANGUAGE") {
            if v.eq_ignore_ascii_case("rust") {
                settings.language = ProgrammingLanguage::Rust;
            } else if v.eq_ignore_ascii_case("java") {
                settings.language = ProgrammingLanguage::Java;
            }
        }

        if let Some(v) = std::env::var_os("CP_BONUS") {
            settings.test_bonus = v != "0"
                && !v.eq_ignore_ascii_case("false")
                && !v.eq_ignore_ascii_case("no")
                && !v.eq_ignore_ascii_case("off")
                && !v.eq_ignore_ascii_case("n")
                && !v.eq_ignore_ascii_case("f");
        }

        Ok(settings)
    }
}
