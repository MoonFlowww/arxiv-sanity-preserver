use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct User {
    pub user_id: i64,
    pub username: String,
    pub pw_hash: String,
    pub creation_time: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct LibraryEntry {
    pub lib_id: i64,
    pub paper_id: String,
    pub user_id: i64,
    pub update_time: Option<i64>,
}

pub struct Database {
    conn: Connection,
}

const SCHEMA_SQL: &str = r#"
create table if not exists user
(
    user_id       integer primary key autoincrement,
    username      text not null,
    pw_hash       text not null,
    creation_time integer
);
create table if not exists library
(
    lib_id      integer primary key autoincrement,
    paper_id    text    not null,
    user_id     integer not null,
    update_time integer
);
"#;

impl Database {
    pub fn open(path: &Path) -> Result<Self, String> {
        let conn = Connection::open(path)
            .map_err(|err| format!("Failed to open database at {path:?}: {err}"))?;
        Ok(Self { conn })
    }

    pub fn initialize_schema(&self) -> Result<(), String> {
        self.conn
            .execute_batch(SCHEMA_SQL)
            .map_err(|err| format!("Failed to initialize schema: {err}"))
    }

    pub fn get_user_by_id(&self, user_id: i64) -> Result<Option<User>, String> {
        self.conn
            .query_row(
                "select user_id, username, pw_hash, creation_time from user where user_id = ?",
                [user_id],
                |row| {
                    Ok(User {
                        user_id: row.get(0)?,
                        username: row.get(1)?,
                        pw_hash: row.get(2)?,
                        creation_time: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(|err| format!("Failed to load user {user_id}: {err}"))
    }

    pub fn get_user_by_username(&self, username: &str) -> Result<Option<User>, String> {
        self.conn
            .query_row(
                "select user_id, username, pw_hash, creation_time from user where username = ?",
                [username],
                |row| {
                    Ok(User {
                        user_id: row.get(0)?,
                        username: row.get(1)?,
                        pw_hash: row.get(2)?,
                        creation_time: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(|err| format!("Failed to load user {username}: {err}"))
    }

    pub fn list_users(&self) -> Result<Vec<User>, String> {
        let mut stmt = self
            .conn
            .prepare("select user_id, username, pw_hash, creation_time from user")
            .map_err(|err| format!("Failed to query users: {err}"))?;
        let iter = stmt
            .query_map([], |row| {
                Ok(User {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    pw_hash: row.get(2)?,
                    creation_time: row.get(3)?,
                })
            })
            .map_err(|err| format!("Failed to iterate users: {err}"))?;
        let mut users = Vec::new();
        for user in iter {
            users.push(user.map_err(|err| format!("Failed to read user row: {err}"))?);
        }
        Ok(users)
    }

    pub fn list_library(&self) -> Result<Vec<LibraryEntry>, String> {
        let mut stmt = self
            .conn
            .prepare("select lib_id, paper_id, user_id, update_time from library")
            .map_err(|err| format!("Failed to query library: {err}"))?;
        let iter = stmt
            .query_map([], |row| {
                Ok(LibraryEntry {
                    lib_id: row.get(0)?,
                    paper_id: row.get(1)?,
                    user_id: row.get(2)?,
                    update_time: row.get(3)?,
                })
            })
            .map_err(|err| format!("Failed to iterate library: {err}"))?;
        let mut entries = Vec::new();
        for entry in iter {
            entries.push(entry.map_err(|err| format!("Failed to read library row: {err}"))?);
        }
        Ok(entries)
    }

    pub fn list_library_for_user(&self, user_id: i64) -> Result<Vec<LibraryEntry>, String> {
        let mut stmt = self
            .conn
            .prepare(
                "select lib_id, paper_id, user_id, update_time from library where user_id = ?",
            )
            .map_err(|err| format!("Failed to query library for user {user_id}: {err}"))?;
        let iter = stmt
            .query_map([user_id], |row| {
                Ok(LibraryEntry {
                    lib_id: row.get(0)?,
                    paper_id: row.get(1)?,
                    user_id: row.get(2)?,
                    update_time: row.get(3)?,
                })
            })
            .map_err(|err| format!("Failed to iterate library for user {user_id}: {err}"))?;
        let mut entries = Vec::new();
        for entry in iter {
            entries.push(entry.map_err(|err| format!("Failed to read library row: {err}"))?);
        }
        Ok(entries)
    }

    pub fn list_library_paper_ids_for_user(&self, user_id: i64) -> Result<Vec<String>, String> {
        let mut stmt = self
            .conn
            .prepare("select paper_id from library where user_id = ?")
            .map_err(|err| format!("Failed to query library for user {user_id}: {err}"))?;
        let iter = stmt
            .query_map([user_id], |row| row.get(0))
            .map_err(|err| format!("Failed to iterate library for user {user_id}: {err}"))?;
        let mut paper_ids = Vec::new();
        for pid in iter {
            paper_ids.push(pid.map_err(|err| format!("Failed to read library row: {err}"))?);
        }
        Ok(paper_ids)
    }

    pub fn count_library_for_user(&self, user_id: i64) -> Result<i64, String> {
        self.conn
            .query_row(
                "select count(*) from library where user_id = ?",
                [user_id],
                |row| row.get(0),
            )
            .map_err(|err| format!("Failed to count library for user {user_id}: {err}"))
    }

    pub fn ensure_user(
        &self,
        user_id: i64,
        username: &str,
        pw_hash: &str,
        creation_time: i64,
    ) -> Result<User, String> {
        if let Some(user) = self.get_user_by_id(user_id)? {
            return Ok(user);
        }
        self.conn
            .execute(
                "insert into user (user_id, username, pw_hash, creation_time) values (?, ?, ?, ?)",
                params![user_id, username, pw_hash, creation_time],
            )
            .map_err(|err| format!("Failed to insert user {username}: {err}"))?;
        self.get_user_by_id(user_id)?
            .ok_or_else(|| format!("Failed to reload user {username} after insert"))
    }

    pub fn toggle_library_entry(
        &self,
        user_id: i64,
        paper_id: &str,
        update_time: i64,
    ) -> Result<bool, String> {
        let existing: Option<i64> = self
            .conn
            .query_row(
                "select lib_id from library where user_id = ? and paper_id = ?",
                params![user_id, paper_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| format!("Failed to query library toggle: {err}"))?;
        if let Some(lib_id) = existing {
            self.conn
                .execute("delete from library where lib_id = ?", [lib_id])
                .map_err(|err| format!("Failed to delete library entry {lib_id}: {err}"))?;
            Ok(false)
        } else {
            self.conn
                .execute(
                    "insert into library (paper_id, user_id, update_time) values (?, ?, ?)",
                    params![paper_id, user_id, update_time],
                )
                .map_err(|err| {
                    format!("Failed to insert library entry for {paper_id}: {err}")
                })?;
            Ok(true)
        }
    }
}
