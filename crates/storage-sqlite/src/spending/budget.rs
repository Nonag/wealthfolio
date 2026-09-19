//! Storage adapter for spending::budget.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db::{get_connection, DbPool, WriteHandle};
use crate::errors::StorageError;
use crate::schema::{
    budget_group_assignments, budget_groups, budget_rollover_settings, budget_targets,
};
use crate::spending::deterministic_ids::{
    budget_group_assignment_id, budget_rollover_setting_id, budget_target_id,
};
use wealthfolio_core::errors::ValidationError;
use wealthfolio_core::sync::{SyncEntity, SyncOperation};
use wealthfolio_spending::budget::{
    BudgetGroup, BudgetGroupAssignment, BudgetRepositoryTrait, BudgetRolloverSetting,
    BudgetRolloverTargetType, BudgetTarget, BudgetTargetType, NewBudgetGroup,
    NewBudgetGroupAssignment, NewBudgetRolloverSetting, NewBudgetTarget, UpdateBudgetGroup,
};

#[derive(Queryable, Identifiable, Selectable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = crate::schema::budget_groups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct BudgetGroupDB {
    pub id: String,
    pub name: String,
    pub key: String,
    pub color: Option<String>,
    pub icon: Option<String>,
    pub sort_order: i32,
    pub is_system: i32,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = crate::schema::budget_groups)]
pub struct NewBudgetGroupDB {
    pub id: String,
    pub name: String,
    pub key: String,
    pub color: Option<String>,
    pub icon: Option<String>,
    pub sort_order: i32,
    pub is_system: i32,
    pub created_at: String,
    pub updated_at: String,
}

impl crate::sync::SyncOutboxModel for BudgetGroupDB {
    const ENTITY: SyncEntity = SyncEntity::BudgetGroup;
    fn sync_entity_id(&self) -> &str {
        &self.id
    }

    fn should_sync_outbox(&self, _op: SyncOperation) -> bool {
        self.is_system == 0 || self.updated_at != self.created_at
    }
}

#[derive(Queryable, Identifiable, Selectable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = crate::schema::budget_group_assignments)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct BudgetGroupAssignmentDB {
    pub id: String,
    pub group_id: String,
    pub taxonomy_id: String,
    pub category_id: String,
    pub is_system: i32,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = crate::schema::budget_group_assignments)]
pub struct NewBudgetGroupAssignmentDB {
    pub id: String,
    pub group_id: String,
    pub taxonomy_id: String,
    pub category_id: String,
    pub is_system: i32,
    pub created_at: String,
    pub updated_at: String,
}

impl crate::sync::SyncOutboxModel for BudgetGroupAssignmentDB {
    const ENTITY: SyncEntity = SyncEntity::BudgetGroupAssignment;
    fn sync_entity_id(&self) -> &str {
        &self.id
    }

    fn should_sync_outbox(&self, _op: SyncOperation) -> bool {
        self.is_system == 0 || self.updated_at != self.created_at
    }
}

#[derive(Queryable, Identifiable, Selectable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = crate::schema::budget_targets)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct BudgetTargetDB {
    pub id: String,
    pub period_key: String,
    pub target_type: String,
    pub taxonomy_id: Option<String>,
    pub category_id: Option<String>,
    pub group_id: Option<String>,
    pub amount: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = crate::schema::budget_targets)]
pub struct NewBudgetTargetDB {
    pub id: String,
    pub period_key: String,
    pub target_type: String,
    pub taxonomy_id: Option<String>,
    pub category_id: Option<String>,
    pub group_id: Option<String>,
    pub amount: String,
    pub created_at: String,
    pub updated_at: String,
}

impl crate::sync::SyncOutboxModel for BudgetTargetDB {
    const ENTITY: SyncEntity = SyncEntity::BudgetTarget;
    fn sync_entity_id(&self) -> &str {
        &self.id
    }
}

#[derive(Queryable, Identifiable, Selectable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = crate::schema::budget_rollover_settings)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct BudgetRolloverSettingDB {
    pub id: String,
    pub target_type: String,
    pub taxonomy_id: Option<String>,
    pub category_id: Option<String>,
    pub group_id: Option<String>,
    pub enabled: i32,
    pub start_month: String,
    pub starting_balance: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = crate::schema::budget_rollover_settings)]
pub struct NewBudgetRolloverSettingDB {
    pub id: String,
    pub target_type: String,
    pub taxonomy_id: Option<String>,
    pub category_id: Option<String>,
    pub group_id: Option<String>,
    pub enabled: i32,
    pub start_month: String,
    pub starting_balance: String,
    pub created_at: String,
    pub updated_at: String,
}

impl crate::sync::SyncOutboxModel for BudgetRolloverSettingDB {
    const ENTITY: SyncEntity = SyncEntity::BudgetRolloverSetting;
    fn sync_entity_id(&self) -> &str {
        &self.id
    }
}

pub struct BudgetRepository {
    pool: Arc<DbPool>,
    writer: WriteHandle,
}

impl BudgetRepository {
    pub fn new(pool: Arc<DbPool>, writer: WriteHandle) -> Self {
        Self { pool, writer }
    }

    async fn upsert_group_assignments_with_system_flag(
        &self,
        assignments: Vec<NewBudgetGroupAssignment>,
        is_system: i32,
    ) -> Result<Vec<BudgetGroupAssignment>> {
        let now = chrono::Utc::now().to_rfc3339();
        self.writer
            .exec_tx(move |tx| {
                let mut out = Vec::with_capacity(assignments.len());
                for assignment in assignments {
                    let NewBudgetGroupAssignment {
                        id,
                        group_id,
                        taxonomy_id,
                        category_id,
                    } = assignment;
                    let id = id
                        .unwrap_or_else(|| budget_group_assignment_id(&taxonomy_id, &category_id));
                    let existing = budget_group_assignments::table
                        .filter(budget_group_assignments::taxonomy_id.eq(&taxonomy_id))
                        .filter(budget_group_assignments::category_id.eq(&category_id))
                        .first::<BudgetGroupAssignmentDB>(tx.conn())
                        .optional()
                        .map_err(StorageError::from)?;
                    if let Some(existing) = existing {
                        if existing.group_id == group_id && existing.is_system == is_system {
                            out.push(existing);
                            continue;
                        }
                        diesel::update(budget_group_assignments::table.find(&existing.id))
                            .set((
                                budget_group_assignments::group_id.eq(&group_id),
                                budget_group_assignments::is_system.eq(is_system),
                                budget_group_assignments::updated_at.eq(&now),
                            ))
                            .execute(tx.conn())
                            .map_err(StorageError::from)?;
                        let updated = budget_group_assignments::table
                            .find(&existing.id)
                            .first::<BudgetGroupAssignmentDB>(tx.conn())
                            .map_err(StorageError::from)?;
                        tx.update(&updated)?;
                        out.push(updated);
                        continue;
                    }

                    let row = NewBudgetGroupAssignmentDB {
                        id,
                        group_id,
                        taxonomy_id,
                        category_id,
                        is_system,
                        created_at: now.clone(),
                        updated_at: now.clone(),
                    };
                    let inserted = diesel::insert_into(budget_group_assignments::table)
                        .values(&row)
                        .returning(BudgetGroupAssignmentDB::as_returning())
                        .get_result(tx.conn())
                        .map_err(StorageError::from)?;
                    tx.update(&inserted)?;
                    out.push(inserted);
                }
                Ok(out)
            })
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(|e| anyhow::anyhow!(e))
    }
}

fn parse_dt(s: &str) -> NaiveDateTime {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.naive_utc())
        .unwrap_or_else(|_| chrono::Utc::now().naive_utc())
}

fn target_type_from_str(value: &str) -> BudgetTargetType {
    match value {
        "group_buffer" => BudgetTargetType::GroupBuffer,
        _ => BudgetTargetType::Category,
    }
}

fn rollover_target_type_from_str(value: &str) -> BudgetRolloverTargetType {
    match value {
        "group" => BudgetRolloverTargetType::Group,
        _ => BudgetRolloverTargetType::Category,
    }
}

impl From<BudgetGroupDB> for BudgetGroup {
    fn from(db: BudgetGroupDB) -> Self {
        Self {
            id: db.id,
            name: db.name,
            key: db.key,
            color: db.color,
            icon: db.icon,
            sort_order: db.sort_order,
            is_system: db.is_system != 0,
            created_at: parse_dt(&db.created_at),
            updated_at: parse_dt(&db.updated_at),
        }
    }
}

impl From<BudgetGroupAssignmentDB> for BudgetGroupAssignment {
    fn from(db: BudgetGroupAssignmentDB) -> Self {
        Self {
            id: db.id,
            group_id: db.group_id,
            taxonomy_id: db.taxonomy_id,
            category_id: db.category_id,
            created_at: parse_dt(&db.created_at),
            updated_at: parse_dt(&db.updated_at),
        }
    }
}

impl From<BudgetTargetDB> for BudgetTarget {
    fn from(db: BudgetTargetDB) -> Self {
        Self {
            id: db.id,
            period_key: db.period_key,
            target_type: target_type_from_str(&db.target_type),
            taxonomy_id: db.taxonomy_id,
            category_id: db.category_id,
            group_id: db.group_id,
            amount: db.amount,
            created_at: parse_dt(&db.created_at),
            updated_at: parse_dt(&db.updated_at),
        }
    }
}

impl From<BudgetRolloverSettingDB> for BudgetRolloverSetting {
    fn from(db: BudgetRolloverSettingDB) -> Self {
        Self {
            id: db.id,
            target_type: rollover_target_type_from_str(&db.target_type),
            taxonomy_id: db.taxonomy_id,
            category_id: db.category_id,
            group_id: db.group_id,
            enabled: db.enabled != 0,
            start_month: db.start_month,
            starting_balance: db.starting_balance,
            created_at: parse_dt(&db.created_at),
            updated_at: parse_dt(&db.updated_at),
        }
    }
}

#[async_trait]
impl BudgetRepositoryTrait for BudgetRepository {
    async fn list_groups(&self) -> Result<Vec<BudgetGroup>> {
        let mut conn = get_connection(&self.pool).map_err(|e| anyhow::anyhow!(e))?;
        let rows = budget_groups::table
            .order((budget_groups::sort_order.asc(), budget_groups::name.asc()))
            .load::<BudgetGroupDB>(&mut conn)
            .map_err(StorageError::from)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn create_group(&self, new_group: NewBudgetGroup) -> Result<BudgetGroup> {
        let now = chrono::Utc::now().to_rfc3339();
        let row = NewBudgetGroupDB {
            id: new_group.id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            key: new_group.key.unwrap_or_else(|| Uuid::new_v4().to_string()),
            name: new_group.name,
            color: new_group.color,
            icon: new_group.icon,
            sort_order: new_group.sort_order.unwrap_or(0),
            is_system: if new_group.is_system { 1 } else { 0 },
            created_at: now.clone(),
            updated_at: now,
        };
        self.writer
            .exec_tx(move |tx| {
                let inserted = diesel::insert_into(budget_groups::table)
                    .values(&row)
                    .returning(BudgetGroupDB::as_returning())
                    .get_result(tx.conn())
                    .map_err(StorageError::from)?;
                tx.insert(&inserted)?;
                Ok(inserted)
            })
            .await
            .map(Into::into)
            .map_err(|e| anyhow::anyhow!(e))
    }

    async fn update_group(&self, id: &str, patch: UpdateBudgetGroup) -> Result<BudgetGroup> {
        let id = id.to_string();
        self.writer
            .exec_tx(move |tx| {
                let mut existing = budget_groups::table
                    .find(&id)
                    .first::<BudgetGroupDB>(tx.conn())
                    .map_err(StorageError::from)?;
                if let Some(name) = patch.name {
                    existing.name = name;
                }
                if let Some(color) = patch.color {
                    existing.color = color;
                }
                if let Some(icon) = patch.icon {
                    existing.icon = icon;
                }
                if let Some(sort_order) = patch.sort_order {
                    existing.sort_order = sort_order;
                }
                existing.updated_at = chrono::Utc::now().to_rfc3339();
                diesel::update(budget_groups::table.find(&id))
                    .set((
                        budget_groups::name.eq(&existing.name),
                        budget_groups::color.eq(&existing.color),
                        budget_groups::icon.eq(&existing.icon),
                        budget_groups::sort_order.eq(existing.sort_order),
                        budget_groups::updated_at.eq(&existing.updated_at),
                    ))
                    .execute(tx.conn())
                    .map_err(StorageError::from)?;
                tx.update(&existing)?;
                Ok(existing)
            })
            .await
            .map(Into::into)
            .map_err(|e| anyhow::anyhow!(e))
    }

    async fn delete_group_and_reassign(&self, id: &str, reassign_to_group_id: &str) -> Result<()> {
        let id = id.to_string();
        let reassign_to_group_id = reassign_to_group_id.to_string();
        let now = chrono::Utc::now().to_rfc3339();
        self.writer
            .exec_tx(move |tx| {
                let group = budget_groups::table
                    .find(&id)
                    .first::<BudgetGroupDB>(tx.conn())
                    .optional()
                    .map_err(StorageError::from)?
                    .ok_or_else(|| ValidationError::InvalidInput("Budget group not found".into()))?;
                if group.key == "other" {
                    return Err(ValidationError::InvalidInput(
                        "The \"Other\" budget group cannot be deleted - it is the default bucket for unassigned categories".into(),
                    ).into());
                }
                if id == reassign_to_group_id {
                    return Err(ValidationError::InvalidInput(
                        "Cannot reassign categories to the group being deleted".into(),
                    ).into());
                }
                if budget_groups::table
                    .find(&reassign_to_group_id)
                    .first::<BudgetGroupDB>(tx.conn())
                    .optional()
                    .map_err(StorageError::from)?
                    .is_none()
                {
                    return Err(ValidationError::InvalidInput("Reassignment budget group not found".into()).into());
                }
                if diesel::select(diesel::dsl::exists(
                    budget_rollover_settings::table
                        .filter(budget_rollover_settings::target_type.eq("group"))
                        .filter(budget_rollover_settings::group_id.eq(&id)),
                ))
                .get_result::<bool>(tx.conn())
                .map_err(StorageError::from)?
                {
                    return Err(ValidationError::InvalidInput(
                        "Delete the group's rollover setting before deleting the group".into(),
                    ).into());
                }

                let assignments = budget_group_assignments::table
                    .filter(budget_group_assignments::group_id.eq(&id))
                    .load::<BudgetGroupAssignmentDB>(tx.conn())
                    .map_err(StorageError::from)?;
                for mut assignment in assignments {
                    assignment.group_id = reassign_to_group_id.clone();
                    assignment.is_system = 0;
                    assignment.updated_at = now.clone();
                    diesel::update(budget_group_assignments::table.find(&assignment.id))
                        .set((
                            budget_group_assignments::group_id.eq(&assignment.group_id),
                            budget_group_assignments::is_system.eq(0),
                            budget_group_assignments::updated_at.eq(&assignment.updated_at),
                        ))
                        .execute(tx.conn())
                        .map_err(StorageError::from)?;
                    tx.update(&assignment)?;
                }

                let source_buffers = budget_targets::table
                    .filter(budget_targets::target_type.eq("group_buffer"))
                    .filter(budget_targets::group_id.eq(&id))
                    .load::<BudgetTargetDB>(tx.conn())
                    .map_err(StorageError::from)?;

                let destination_buffers = budget_targets::table
                    .filter(budget_targets::target_type.eq("group_buffer"))
                    .filter(budget_targets::group_id.eq(&reassign_to_group_id))
                    .load::<BudgetTargetDB>(tx.conn())
                    .map_err(StorageError::from)?;
                let periods = source_buffers
                    .iter()
                    .chain(&destination_buffers)
                    .map(|row| row.period_key.as_str())
                    .collect::<BTreeSet<_>>();
                // Monthly overrides replace defaults, so sum each side's effective
                // value from the original rows, not the partially merged destination.
                let effective = |rows: &[BudgetTargetDB], period: &str| {
                    rows.iter()
                        .find(|row| row.period_key == period)
                        .or_else(|| rows.iter().find(|row| row.period_key == "default"))
                        .and_then(|row| row.amount.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO)
                };
                for period in periods {
                    let amount = (effective(&source_buffers, period)
                        + effective(&destination_buffers, period))
                        .normalize()
                        .to_string();
                    let destination = destination_buffers
                        .iter()
                        .find(|row| row.period_key == period);

                    if let Some(destination) = destination {
                        let mut destination = destination.clone();
                        destination.amount = amount;
                        destination.updated_at = now.clone();
                        diesel::update(budget_targets::table.find(&destination.id))
                            .set((
                                budget_targets::amount.eq(&destination.amount),
                                budget_targets::updated_at.eq(&destination.updated_at),
                            ))
                            .execute(tx.conn())
                            .map_err(StorageError::from)?;
                        tx.update(&destination)?;
                    } else {
                        let row = NewBudgetTargetDB {
                            // A fresh ID avoids reusing a deleted destination's sync tombstone.
                            id: Uuid::new_v4().to_string(),
                            period_key: period.to_string(),
                            target_type: "group_buffer".to_string(),
                            taxonomy_id: None,
                            category_id: None,
                            group_id: Some(reassign_to_group_id.clone()),
                            amount,
                            created_at: now.clone(),
                            updated_at: now.clone(),
                        };
                        let inserted = diesel::insert_into(budget_targets::table)
                            .values(&row)
                            .returning(BudgetTargetDB::as_returning())
                            .get_result(tx.conn())
                            .map_err(StorageError::from)?;
                        tx.update(&inserted)?;
                    }
                }
                for source in source_buffers {
                    diesel::delete(budget_targets::table.find(&source.id))
                        .execute(tx.conn())
                        .map_err(StorageError::from)?;
                    tx.delete::<BudgetTargetDB>(source.id);
                }

                let affected = diesel::delete(budget_groups::table.find(&id))
                    .execute(tx.conn())
                    .map_err(StorageError::from)?;
                if affected > 0 {
                    tx.delete::<BudgetGroupDB>(id.clone());
                }
                Ok(())
            })
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    async fn list_group_assignments(&self) -> Result<Vec<BudgetGroupAssignment>> {
        let mut conn = get_connection(&self.pool).map_err(|e| anyhow::anyhow!(e))?;
        let rows = budget_group_assignments::table
            .load::<BudgetGroupAssignmentDB>(&mut conn)
            .map_err(StorageError::from)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn upsert_group_assignment(
        &self,
        assignment: NewBudgetGroupAssignment,
    ) -> Result<BudgetGroupAssignment> {
        let rows = self.upsert_group_assignments(vec![assignment]).await?;
        rows.into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Failed to save budget group assignment"))
    }

    async fn upsert_group_assignments(
        &self,
        assignments: Vec<NewBudgetGroupAssignment>,
    ) -> Result<Vec<BudgetGroupAssignment>> {
        self.upsert_group_assignments_with_system_flag(assignments, 0)
            .await
    }

    async fn list_targets(&self) -> Result<Vec<BudgetTarget>> {
        let mut conn = get_connection(&self.pool).map_err(|e| anyhow::anyhow!(e))?;
        let rows = budget_targets::table
            .load::<BudgetTargetDB>(&mut conn)
            .map_err(StorageError::from)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn upsert_target(&self, target: NewBudgetTarget) -> Result<BudgetTarget> {
        let now = chrono::Utc::now().to_rfc3339();
        let NewBudgetTarget {
            id,
            period_key,
            target_type,
            taxonomy_id,
            category_id,
            group_id,
            amount,
        } = target;
        let target_type = target_type.as_str().to_string();
        let id = id.unwrap_or_else(|| {
            budget_target_id(
                &period_key,
                &target_type,
                taxonomy_id.as_deref(),
                category_id.as_deref(),
                group_id.as_deref(),
            )
        });
        let row = NewBudgetTargetDB {
            id,
            period_key,
            target_type,
            taxonomy_id,
            category_id,
            group_id,
            amount,
            created_at: now.clone(),
            updated_at: now,
        };
        self.writer
            .exec_tx(move |tx| {
                let existing_id = if row.target_type == "category" {
                    budget_targets::table
                        .filter(budget_targets::target_type.eq("category"))
                        .filter(budget_targets::period_key.eq(&row.period_key))
                        .filter(budget_targets::taxonomy_id.eq(&row.taxonomy_id))
                        .filter(budget_targets::category_id.eq(&row.category_id))
                        .select(budget_targets::id)
                        .first::<String>(tx.conn())
                        .optional()
                        .map_err(StorageError::from)?
                } else {
                    budget_targets::table
                        .filter(budget_targets::target_type.eq("group_buffer"))
                        .filter(budget_targets::period_key.eq(&row.period_key))
                        .filter(budget_targets::group_id.eq(&row.group_id))
                        .select(budget_targets::id)
                        .first::<String>(tx.conn())
                        .optional()
                        .map_err(StorageError::from)?
                };
                let result = if let Some(existing_id) = existing_id {
                    diesel::update(budget_targets::table.find(&existing_id))
                        .set((
                            budget_targets::amount.eq(&row.amount),
                            budget_targets::updated_at.eq(&row.updated_at),
                        ))
                        .execute(tx.conn())
                        .map_err(StorageError::from)?;
                    budget_targets::table
                        .find(&existing_id)
                        .first::<BudgetTargetDB>(tx.conn())
                        .map_err(StorageError::from)?
                } else {
                    diesel::insert_into(budget_targets::table)
                        .values(&row)
                        .returning(BudgetTargetDB::as_returning())
                        .get_result(tx.conn())
                        .map_err(StorageError::from)?
                };
                tx.update(&result)?;
                Ok(result)
            })
            .await
            .map(Into::into)
            .map_err(|e| anyhow::anyhow!(e))
    }

    async fn delete_target(&self, id: &str) -> Result<()> {
        let id = id.to_string();
        self.writer
            .exec_tx(move |tx| {
                let affected = diesel::delete(budget_targets::table.find(&id))
                    .execute(tx.conn())
                    .map_err(StorageError::from)?;
                if affected > 0 {
                    tx.delete::<BudgetTargetDB>(id.clone());
                }
                Ok(())
            })
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    async fn list_rollover_settings(&self) -> Result<Vec<BudgetRolloverSetting>> {
        let mut conn = get_connection(&self.pool).map_err(|e| anyhow::anyhow!(e))?;
        let rows = budget_rollover_settings::table
            .load::<BudgetRolloverSettingDB>(&mut conn)
            .map_err(StorageError::from)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn upsert_rollover_setting(
        &self,
        setting: NewBudgetRolloverSetting,
    ) -> Result<BudgetRolloverSetting> {
        let now = chrono::Utc::now().to_rfc3339();
        let NewBudgetRolloverSetting {
            id,
            target_type,
            taxonomy_id,
            category_id,
            group_id,
            enabled,
            start_month,
            starting_balance,
        } = setting;
        let target_type = target_type.as_str().to_string();
        let id = id.unwrap_or_else(|| {
            budget_rollover_setting_id(
                &target_type,
                taxonomy_id.as_deref(),
                category_id.as_deref(),
                group_id.as_deref(),
            )
        });
        let row = NewBudgetRolloverSettingDB {
            id,
            target_type,
            taxonomy_id,
            category_id,
            group_id,
            enabled: if enabled { 1 } else { 0 },
            start_month,
            starting_balance,
            created_at: now.clone(),
            updated_at: now,
        };
        self.writer
            .exec_tx(move |tx| {
                let existing_id = if row.target_type == "category" {
                    budget_rollover_settings::table
                        .filter(budget_rollover_settings::target_type.eq("category"))
                        .filter(budget_rollover_settings::taxonomy_id.eq(&row.taxonomy_id))
                        .filter(budget_rollover_settings::category_id.eq(&row.category_id))
                        .select(budget_rollover_settings::id)
                        .first::<String>(tx.conn())
                        .optional()
                        .map_err(StorageError::from)?
                } else {
                    budget_rollover_settings::table
                        .filter(budget_rollover_settings::target_type.eq("group"))
                        .filter(budget_rollover_settings::group_id.eq(&row.group_id))
                        .select(budget_rollover_settings::id)
                        .first::<String>(tx.conn())
                        .optional()
                        .map_err(StorageError::from)?
                };
                let result = if let Some(existing_id) = existing_id {
                    diesel::update(budget_rollover_settings::table.find(&existing_id))
                        .set((
                            budget_rollover_settings::enabled.eq(row.enabled),
                            budget_rollover_settings::start_month.eq(&row.start_month),
                            budget_rollover_settings::starting_balance.eq(&row.starting_balance),
                            budget_rollover_settings::updated_at.eq(&row.updated_at),
                        ))
                        .execute(tx.conn())
                        .map_err(StorageError::from)?;
                    budget_rollover_settings::table
                        .find(&existing_id)
                        .first::<BudgetRolloverSettingDB>(tx.conn())
                        .map_err(StorageError::from)?
                } else {
                    diesel::insert_into(budget_rollover_settings::table)
                        .values(&row)
                        .returning(BudgetRolloverSettingDB::as_returning())
                        .get_result(tx.conn())
                        .map_err(StorageError::from)?
                };
                tx.update(&result)?;
                Ok(result)
            })
            .await
            .map(Into::into)
            .map_err(|e| anyhow::anyhow!(e))
    }

    async fn delete_rollover_setting(&self, id: &str) -> Result<()> {
        let id = id.to_string();
        self.writer
            .exec_tx(move |tx| {
                let affected = diesel::delete(budget_rollover_settings::table.find(&id))
                    .execute(tx.conn())
                    .map_err(StorageError::from)?;
                if affected > 0 {
                    tx.delete::<BudgetRolloverSettingDB>(id.clone());
                }
                Ok(())
            })
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    async fn disable_category_rollovers(
        &self,
        taxonomy_id: &str,
        category_ids: &[String],
    ) -> Result<Vec<BudgetRolloverSetting>> {
        if category_ids.is_empty() {
            return Ok(Vec::new());
        }
        let taxonomy_id = taxonomy_id.to_string();
        let category_ids = category_ids.to_vec();
        self.writer
            .exec_tx(move |tx| {
                let now = chrono::Utc::now().to_rfc3339();
                diesel::update(
                    budget_rollover_settings::table
                        .filter(budget_rollover_settings::target_type.eq("category"))
                        .filter(budget_rollover_settings::taxonomy_id.eq(&taxonomy_id))
                        .filter(budget_rollover_settings::category_id.eq_any(&category_ids)),
                )
                .set((
                    budget_rollover_settings::enabled.eq(0),
                    budget_rollover_settings::updated_at.eq(&now),
                ))
                .execute(tx.conn())
                .map_err(StorageError::from)?;

                let rows = budget_rollover_settings::table
                    .filter(budget_rollover_settings::target_type.eq("category"))
                    .filter(budget_rollover_settings::taxonomy_id.eq(&taxonomy_id))
                    .filter(budget_rollover_settings::category_id.eq_any(&category_ids))
                    .load::<BudgetRolloverSettingDB>(tx.conn())
                    .map_err(StorageError::from)?;
                for row in &rows {
                    tx.update(row)?;
                }
                Ok(rows)
            })
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(|e| anyhow::anyhow!(e))
    }

    async fn copy_period_targets(
        &self,
        source_period_key: &str,
        target_period_key: &str,
        overwrite: bool,
    ) -> Result<Vec<BudgetTarget>> {
        let source = source_period_key.to_string();
        let target = target_period_key.to_string();
        self.writer
            .exec_tx(move |tx| {
                let source_rows = budget_targets::table
                    .filter(budget_targets::period_key.eq(&source))
                    .load::<BudgetTargetDB>(tx.conn())
                    .map_err(StorageError::from)?;

                if overwrite {
                    let to_delete = budget_targets::table
                        .filter(budget_targets::period_key.eq(&target))
                        .load::<BudgetTargetDB>(tx.conn())
                        .map_err(StorageError::from)?;
                    diesel::delete(
                        budget_targets::table.filter(budget_targets::period_key.eq(&target)),
                    )
                    .execute(tx.conn())
                    .map_err(StorageError::from)?;
                    for row in &to_delete {
                        tx.delete::<BudgetTargetDB>(row.id.clone());
                    }
                }

                let now = chrono::Utc::now().to_rfc3339();
                for source_row in source_rows {
                    let id = budget_target_id(
                        &target,
                        &source_row.target_type,
                        source_row.taxonomy_id.as_deref(),
                        source_row.category_id.as_deref(),
                        source_row.group_id.as_deref(),
                    );
                    let new_row = NewBudgetTargetDB {
                        id,
                        period_key: target.clone(),
                        target_type: source_row.target_type.clone(),
                        taxonomy_id: source_row.taxonomy_id.clone(),
                        category_id: source_row.category_id.clone(),
                        group_id: source_row.group_id.clone(),
                        amount: source_row.amount.clone(),
                        created_at: now.clone(),
                        updated_at: now.clone(),
                    };
                    let inserted = diesel::insert_into(budget_targets::table)
                        .values(&new_row)
                        .on_conflict_do_nothing()
                        .returning(BudgetTargetDB::as_returning())
                        .get_result(tx.conn())
                        .optional()
                        .map_err(StorageError::from)?;
                    if let Some(row) = inserted {
                        tx.update(&row)?;
                    }
                }

                let all_target_rows = budget_targets::table
                    .filter(budget_targets::period_key.eq(&target))
                    .load::<BudgetTargetDB>(tx.conn())
                    .map_err(StorageError::from)?;
                Ok(all_target_rows)
            })
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(|e| anyhow::anyhow!(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{create_pool, run_migrations, write_actor::spawn_writer};

    const SOURCE: &str = "032ecb02-5912-42e8-9724-2cd566fc08d5";
    const DESTINATION: &str = "6e25d097-0c73-4521-9407-d47e8dfb73e2";
    const UNRELATED: &str = "a409e0d6-9152-49c8-a5b4-a147a8ac636e";

    fn fixture() -> (tempfile::TempDir, BudgetRepository, rusqlite::Connection) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budget.db");
        run_migrations(path.to_str().unwrap()).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        let pool = create_pool(path.to_str().unwrap()).unwrap();
        let writer = spawn_writer((*pool).clone()).unwrap();
        (dir, BudgetRepository::new(pool, writer), conn)
    }

    async fn buffer(repo: &BudgetRepository, group: &str, period: &str, amount: &str) {
        repo.upsert_target(NewBudgetTarget {
            id: None,
            period_key: period.to_string(),
            target_type: BudgetTargetType::GroupBuffer,
            taxonomy_id: None,
            category_id: None,
            group_id: Some(group.to_string()),
            amount: amount.to_string(),
        })
        .await
        .unwrap();
    }

    async fn state(repo: &BudgetRepository) -> serde_json::Value {
        serde_json::json!({
            "groups": repo.list_groups().await.unwrap(),
            "assignments": repo.list_group_assignments().await.unwrap(),
            "targets": repo.list_targets().await.unwrap(),
            "rollovers": repo.list_rollover_settings().await.unwrap(),
        })
    }

    #[tokio::test]
    async fn delete_group_merges_defaults_and_monthly_overrides() {
        for (source_default, destination_default) in
            [(true, true), (true, false), (false, true), (false, false)]
        {
            let (_dir, repo, _conn) = fixture();
            if source_default {
                buffer(&repo, SOURCE, "default", "0.1").await;
            }
            if destination_default {
                buffer(&repo, DESTINATION, "default", "0.2").await;
            }
            buffer(&repo, SOURCE, "2026-01", "0").await;
            buffer(&repo, SOURCE, "2026-02", "1.123456789012345678").await;
            buffer(&repo, DESTINATION, "2026-02", "2.987654321098765432").await;
            buffer(&repo, DESTINATION, "2026-03", "0").await;
            let before = repo.list_targets().await.unwrap();
            repo.delete_group_and_reassign(SOURCE, DESTINATION)
                .await
                .unwrap();
            let targets = repo.list_targets().await.unwrap();
            let source = Decimal::new(i64::from(source_default), 1);
            let destination = Decimal::new(2 * i64::from(destination_default), 1);
            for (period, expected) in [
                ("default", source + destination),
                ("2026-01", destination),
                ("2026-02", "4.11111111011111111".parse().unwrap()),
                ("2026-03", source),
                ("2026-04", source + destination),
            ] {
                let actual = targets
                    .iter()
                    .find(|t| t.period_key == period)
                    .or_else(|| targets.iter().find(|t| t.period_key == "default"))
                    .map(|t| t.amount.parse::<Decimal>().unwrap())
                    .unwrap_or(Decimal::ZERO);
                assert_eq!(actual, expected, "{period}");
            }
            for target in targets {
                assert_eq!(target.group_id.as_deref(), Some(DESTINATION));
                assert!(!before
                    .iter()
                    .any(|row| { row.group_id.as_deref() == Some(SOURCE) && row.id == target.id }));
                if let Some(existing) = before.iter().find(|row| {
                    row.group_id.as_deref() == Some(DESTINATION)
                        && row.period_key == target.period_key
                }) {
                    assert_eq!(target.id, existing.id);
                }
            }
            let groups = repo.list_groups().await.unwrap();
            assert!(!groups.iter().any(|g| g.id == SOURCE));
            repo.writer.shutdown().await;
        }
    }

    #[tokio::test]
    async fn delete_group_does_not_reuse_deleted_destination_buffer_id() {
        let (_dir, repo, _conn) = fixture();
        buffer(&repo, DESTINATION, "default", "42").await;
        let deleted_id = repo.list_targets().await.unwrap().remove(0).id;
        repo.delete_target(&deleted_id).await.unwrap();
        buffer(&repo, SOURCE, "default", "1.123456789012345678").await;
        let source_id = repo.list_targets().await.unwrap().remove(0).id;

        repo.delete_group_and_reassign(SOURCE, DESTINATION)
            .await
            .unwrap();

        let targets = repo.list_targets().await.unwrap();
        assert_eq!(targets.len(), 1);
        let target = &targets[0];
        assert_eq!(target.group_id.as_deref(), Some(DESTINATION));
        assert_eq!(target.period_key, "default");
        assert_ne!(target.id, deleted_id);
        assert_ne!(target.id, source_id);
        assert_eq!(target.amount, "1.123456789012345678");
        repo.writer.shutdown().await;
    }

    #[tokio::test]
    async fn delete_group_reads_current_assignments_and_preserves_category_state() {
        let (_dir, repo, conn) = fixture();
        for (category, group) in [("cat_housing", UNRELATED), ("cat_food", SOURCE)] {
            repo.upsert_group_assignment(NewBudgetGroupAssignment {
                id: None,
                group_id: group.to_string(),
                taxonomy_id: "spending_categories".to_string(),
                category_id: category.to_string(),
            })
            .await
            .unwrap();
        }
        conn.execute_batch(
            "INSERT INTO budget_targets(id,period_key,target_type,taxonomy_id,category_id,amount)
             VALUES ('category-target','default','category','spending_categories','cat_food','123.456789');
             INSERT INTO budget_rollover_settings(id,target_type,taxonomy_id,category_id,enabled,start_month,starting_balance)
             VALUES ('category-rollover','category','spending_categories','cat_food',1,'2026-01','12.34');",
        ).unwrap();
        buffer(&repo, UNRELATED, "default", "56.78").await;
        let before = state(&repo).await;
        let mut assignments = repo.list_group_assignments().await.unwrap();
        repo.delete_group_and_reassign(SOURCE, DESTINATION)
            .await
            .unwrap();
        let after = state(&repo).await;
        assert_eq!(before["targets"], after["targets"]);
        assert_eq!(before["rollovers"], after["rollovers"]);
        let updated = repo.list_group_assignments().await.unwrap();
        assert_eq!(assignments.len(), updated.len());
        for assignment in &mut assignments {
            let row = updated.iter().find(|row| row.id == assignment.id).unwrap();
            if assignment.group_id == SOURCE {
                assignment.group_id = DESTINATION.to_string();
                assignment.updated_at = row.updated_at;
            }
            assert_eq!(
                serde_json::to_value(row).unwrap(),
                serde_json::to_value(assignment).unwrap()
            );
        }
        repo.writer.shutdown().await;
    }

    #[tokio::test]
    async fn delete_group_guards_leave_state_unchanged() {
        let (_dir, repo, conn) = fixture();
        conn.execute(
            "INSERT INTO budget_rollover_settings(id,target_type,group_id,enabled,start_month,starting_balance)
             VALUES ('rollover','group',?1,0,'2026-01','0')", [SOURCE],
        ).unwrap();
        let before = state(&repo).await;
        conn.execute("DELETE FROM sync_outbox", []).unwrap();
        for (source, destination, message) in [
            (
                DESTINATION,
                SOURCE,
                "The \"Other\" budget group cannot be deleted",
            ),
            (
                SOURCE,
                SOURCE,
                "Cannot reassign categories to the group being deleted",
            ),
            (SOURCE, "missing", "Reassignment budget group not found"),
            ("missing", DESTINATION, "Budget group not found"),
            (SOURCE, DESTINATION, "Delete the group's rollover setting"),
        ] {
            let error = repo
                .delete_group_and_reassign(source, destination)
                .await
                .unwrap_err();
            assert!(error.to_string().contains(message), "{error}");
            assert_eq!(state(&repo).await, before);
            let count: i64 = conn
                .query_row("SELECT count(*) FROM sync_outbox", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 0);
        }
        repo.delete_rollover_setting("rollover").await.unwrap();
        repo.delete_group_and_reassign(SOURCE, DESTINATION)
            .await
            .unwrap();
        repo.writer.shutdown().await;
    }
}
