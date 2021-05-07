use rusqlite::*;

pub mod dht_arc;
use dht_arc::*;

#[derive(Debug)]
pub struct SplitArc {
    pub start_1: Option<u32>,
    pub end_1: Option<u32>,
    pub start_2: Option<u32>,
    pub end_2: Option<u32>,
}

impl From<DhtArc> for SplitArc {
    fn from(f: DhtArc) -> Self {
        use std::ops::{Bound, RangeBounds};
        let r = f.range();
        let s = r.start_bound();
        let e = r.end_bound();
        match (s, e) {
            (Bound::Excluded(_), Bound::Excluded(_)) => {
                // DhtArc only returns excluded bounds in the zero len case
                Self {
                    start_1: None,
                    end_1: None,
                    start_2: None,
                    end_2: None,
                }
            }
            (Bound::Included(s), Bound::Included(e)) => {
                if s > e {
                    Self {
                        start_1: Some(u32::MIN),
                        end_1: Some(*e),
                        start_2: Some(*s),
                        end_2: Some(u32::MAX),
                    }
                } else {
                    Self {
                        start_1: Some(*s),
                        end_1: Some(*e),
                        start_2: None,
                        end_2: None,
                    }
                }
            }
            // this is a quirc of how DhtArc is implemented
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AgentInfo {
    key: [u8; 32],
    signed_at_ms: u64,
    storage_arc: DhtArc,
}

impl AgentInfo {
    pub fn new_rand() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let mut key = [0; 32];
        rng.fill(&mut key[..]);

        let signed_at_ms: u32 = rng.gen();
        let center_loc: u32 = rng.gen();
        let half_length = rng.gen_range(0..u32::MAX / 2);

        Self {
            key,
            signed_at_ms: signed_at_ms as u64,
            storage_arc: DhtArc::new(center_loc, half_length),
        }
    }
}

fn insert(con: &Connection, agent_info: &AgentInfo) -> Result<()> {
    let key = &agent_info.key[..];
    let blob =
        rmp_serde::to_vec_named(agent_info).map_err(|e| Error::ToSqlConversionFailure(e.into()))?;
    let signed_at_ms = agent_info.signed_at_ms;
    let center_loc: u32 = agent_info.storage_arc.center_loc.into();
    let half_length = agent_info.storage_arc.half_length;
    let split_arc: SplitArc = agent_info.storage_arc.clone().into();
    //println!("{:?}", split_arc);
    let SplitArc {
        start_1,
        end_1,
        start_2,
        end_2,
    } = split_arc;
    con.execute(
        "INSERT INTO p2p_store (
            key, blob, signed_at_ms, center_loc, half_length,
            arc_start1, arc_end1, arc_start2, arc_end2
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
        params![
            key,
            blob,
            signed_at_ms,
            center_loc,
            half_length,
            start_1,
            end_1,
            start_2,
            end_2,
        ],
    )?;
    Ok(())
}

fn dump(con: &Connection) -> Result<()> {
    let mut stmt = con.prepare("SELECT * FROM p2p_store")?;
    let _ = stmt
        .query_map([], |r| {
            let s1: Option<u32> = r.get(5)?;
            let e1: Option<u32> = r.get(6)?;
            let s2: Option<u32> = r.get(7)?;
            let e2: Option<u32> = r.get(8)?;
            println!("{:?}-{:?} + {:?}-{:?}", s1, e1, s2, e2);
            Ok(())
        })?
        .collect::<Vec<_>>();
    Ok(())
}

fn count_agents_covering_loc(con: &Connection, loc: u32) -> Result<usize> {
    let mut stmt = con.prepare(
        "SELECT COUNT(key)
        FROM p2p_store
        WHERE (
            arc_start1 IS NOT NULL
            AND arc_end1 IS NOT NULL
            AND ?1 >= arc_start1
            AND ?1 <= arc_end1
        )
        OR (
            arc_start2 IS NOT NULL
            AND arc_end2 IS NOT NULL
            AND ?1 >= arc_start2
            AND ?1 <= arc_end2
        );",
    )?;
    stmt.query_row(params![loc], |r| r.get(0))
}

fn count_agents_overlaping_arc(con: &Connection, arc: DhtArc) -> Result<usize> {
    let split_arc: SplitArc = arc.into();
    let SplitArc {
        start_1,
        end_1,
        start_2,
        end_2,
    } = split_arc;
    let mut stmt = con.prepare(
        "SELECT COUNT(key)
        FROM p2p_store
        WHERE (
            ?1 IS NOT NULL
            AND ?2 IS NOT NULL
            AND arc_start1 IS NOT NULL
            AND arc_end1 IS NOT NULL
            AND ?1 <= arc_end1
            AND ?2 >= arc_start1
        )
        OR (
            ?3 IS NOT NULL
            AND ?4 IS NOT NULL
            AND arc_start2 IS NOT NULL
            AND arc_end2 IS NOT NULL
            AND ?3 <= arc_end2
            AND ?4 >= arc_start2
        );",
    )?;
    stmt.query_row(params![start_1, end_1, start_2, end_2], |r| r.get(0))
}

fn main() -> Result<()> {
    let con = Connection::open_in_memory()?;

    con.execute(
        "CREATE TABLE IF NOT EXISTS p2p_store (
            key             BLOB    PRIMARY KEY ON CONFLICT REPLACE,
            blob            BLOB    NOT NULL,
            signed_at_ms    INTEGER NOT NULL,
            center_loc      INTEGER NOT NULL,
            half_length     INTEGER NOT NULL,
            arc_start1      INTEGER NULL,
            arc_end1        INTEGER NULL,
            arc_start2      INTEGER NULL,
            arc_end2        INTEGER NULL
        );",
        [],
    )?;

    let mut info_zero = AgentInfo::new_rand();
    info_zero.storage_arc = DhtArc::new(0, 0);
    insert(&con, &info_zero)?;

    for _ in 0..10 {
        let agent_info = AgentInfo::new_rand();
        insert(&con, &agent_info)?;
    }

    dump(&con)?;
    println!("agents covering 0: {}", count_agents_covering_loc(&con, 0)?);
    println!(
        "agents covering {}: {}",
        u32::MAX,
        count_agents_covering_loc(&con, u32::MAX)?
    );
    let mid = u32::MAX / 2;
    println!(
        "agents covering {}: {}",
        mid,
        count_agents_covering_loc(&con, mid)?
    );
    let overlap = DhtArc::new(mid, u32::MAX / 4);
    let res = count_agents_overlaping_arc(&con, overlap.clone())?;
    println!("agents overlapping {:?}: {}", &overlap, res);

    Ok(())
}
