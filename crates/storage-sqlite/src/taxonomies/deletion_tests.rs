use super::TaxonomyRepository;
use crate::db::{write_actor::spawn_writer, DbAccess, WriteHandle};
use rusqlite::Connection;
use wealthfolio_core::taxonomies::TaxonomyRepositoryTrait;

// Each statement uses the same composite category key and an independently blocking row.
const REFERENCES: &[(&str, &str, &str)] = &[
    (
        "asset_taxonomy_assignments",
        "INSERT INTO asset_taxonomy_assignments(id,asset_id,taxonomy_id,category_id,weight)
         VALUES ('reference','asset',?1,?2,7500)",
        "asset assignments",
    ),
    (
        "activity_taxonomy_assignments",
        "INSERT INTO activity_taxonomy_assignments(id,activity_id,taxonomy_id,category_id)
         VALUES ('reference','activity',?1,?2)",
        "spending references (activity assignments)",
    ),
    (
        "spending_activity_splits",
        "INSERT INTO spending_activity_splits(id,activity_id,taxonomy_id,category_id,amount,note)
         VALUES ('reference','activity',?1,?2,'100.123456789','Keep split detail')",
        "spending references (transaction splits)",
    ),
    (
        "budget_targets",
        "INSERT INTO budget_targets(id,period_key,target_type,taxonomy_id,category_id,amount)
         VALUES ('reference','2026-09','category',?1,?2,'123.456789')",
        "spending references (budget targets)",
    ),
    (
        "budget_rollover_settings",
        "INSERT INTO budget_rollover_settings(id,target_type,taxonomy_id,category_id,enabled,start_month,starting_balance)
         VALUES ('reference','category',?1,?2,0,'2026-09','-12.3456789')",
        "spending references (rollover settings)",
    ),
    (
        "spending_categorization_rules",
        "INSERT INTO spending_categorization_rules(id,name,pattern,taxonomy_id,category_id)
         VALUES ('reference','Keep rule','merchant',?1,?2)",
        "spending references (categorization rules)",
    ),
    (
        "allocation_target_weights",
        "INSERT INTO allocation_target_weights(id,target_id,taxonomy_id,category_id,target_bps)
         VALUES ('reference',?1,?1,?2,0)",
        "allocation target references",
    ),
];

fn fixture() -> (
    tempfile::TempDir,
    Connection,
    TaxonomyRepository,
    WriteHandle,
) {
    let dir = tempfile::tempdir().unwrap();
    let access = DbAccess::plaintext(dir.path().join("taxonomy.db").to_str().unwrap());
    access.prepare().unwrap();
    access.run_migrations().unwrap();
    let conn = access.connect_rusqlite().unwrap();
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
         INSERT INTO accounts(id,name,currency) VALUES ('account','Account','USD');
         INSERT INTO assets(id,kind,quote_mode,quote_ccy)
             VALUES ('asset','INVESTMENT','MANUAL','USD');
         INSERT INTO activities(id,account_id,activity_type,activity_date,amount,currency,created_at,updated_at)
             VALUES ('activity','account','WITHDRAWAL','2026-09-01T00:00:00Z','100.123456789','USD',
                     '2026-09-01T00:00:00Z','2026-09-01T00:00:00Z');
         INSERT INTO budget_groups(id,name,key) VALUES ('test-group','Test group','test-group');",
    )
    .unwrap();
    for taxonomy in ["spending_categories", "savings_categories"] {
        for (category, parent) in [
            ("test-root", None),
            ("test-child", Some("test-root")),
            ("test-grandchild", Some("test-child")),
            ("test-sibling", None),
        ] {
            conn.execute(
                "INSERT INTO taxonomy_categories(id,taxonomy_id,parent_id,name,key)
                 VALUES (?1,?2,?3,?1,?1)",
                rusqlite::params![category, taxonomy, parent],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO allocation_targets(id,name,scope_type,taxonomy_id)
             VALUES (?1,'Target','all',?1)",
            [taxonomy],
        )
        .unwrap();
    }
    let pool = access.create_pool().unwrap();
    let writer = spawn_writer((*pool).clone()).unwrap();
    let repo = TaxonomyRepository::new(pool, writer.clone());
    (dir, conn, repo, writer)
}

#[tokio::test]
async fn every_reference_blocks_root_child_and_grandchild_without_data_loss() {
    let (_dir, conn, repo, writer) = fixture();
    for &(table, insert, kind) in REFERENCES {
        for category in ["test-root", "test-child", "test-grandchild"] {
            conn.execute(insert, ["spending_categories", category])
                .unwrap();
            let error = repo
                .delete_category("spending_categories", "test-root")
                .await
                .unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("Invalid input: Cannot delete category with 1 {kind}"),
                "{table} at {category}"
            );
            for id in ["test-root", "test-child", "test-grandchild"] {
                assert!(repo
                    .get_category("spending_categories", id)
                    .unwrap()
                    .is_some());
            }
            if table == "spending_activity_splits" {
                let split: (String, String, String, String) = conn.query_row(
                    "SELECT s.category_id,s.amount,s.note,a.amount FROM spending_activity_splits s
                     JOIN activities a ON a.id = s.activity_id WHERE s.id = 'reference'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                ).unwrap();
                assert_eq!(
                    split,
                    (
                        category.to_string(),
                        "100.123456789".to_string(),
                        "Keep split detail".to_string(),
                        "100.123456789".to_string(),
                    )
                );
            }
            let remaining = conn
                .execute(&format!("DELETE FROM {table} WHERE id = 'reference'"), [])
                .unwrap();
            assert_eq!(remaining, 1, "{table} at {category}");
        }
    }
    writer.shutdown().await;
}

#[tokio::test]
async fn clean_subtree_cascades_group_assignments_and_ignores_unrelated_references() {
    let (_dir, conn, repo, writer) = fixture();
    for &(_, insert, _) in REFERENCES {
        conn.execute(insert, ["savings_categories", "test-grandchild"])
            .unwrap();
        conn.execute(
            &insert.replace("'reference'", "'sibling-reference'"),
            ["spending_categories", "test-sibling"],
        )
        .unwrap();
    }
    conn.execute_batch(
        "INSERT INTO budget_group_assignments(id,group_id,taxonomy_id,category_id)
         SELECT taxonomy_id || id,'test-group',taxonomy_id,id FROM taxonomy_categories
         WHERE id LIKE 'test-%';",
    )
    .unwrap();
    assert_eq!(
        repo.delete_category("spending_categories", "test-root")
            .await
            .unwrap(),
        1
    );
    for category in ["test-root", "test-child", "test-grandchild"] {
        assert!(repo
            .get_category("spending_categories", category)
            .unwrap()
            .is_none());
        assert!(repo
            .get_category("savings_categories", category)
            .unwrap()
            .is_some());
    }
    assert!(repo
        .get_category("spending_categories", "test-sibling")
        .unwrap()
        .is_some());
    let remaining: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM budget_group_assignments WHERE group_id = 'test-group'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 5);
    writer.shutdown().await;
}
