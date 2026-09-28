use crate::profiles::ProfileAccess;

use tauri::AppHandle;
use wealthfolio_core::addons::network::{
    resolve_addon_network_auth_header, AddonNetworkRequest, AddonNetworkResponse,
};
use wealthfolio_core::addons::AddonServiceTrait;

#[tauri::command]
pub async fn addon_network_request(
    _app_handle: AppHandle,
    state: ProfileAccess,
    addon_id: String,
    mut request: AddonNetworkRequest,
) -> Result<AddonNetworkResponse, String> {
    let context = state.context()?;
    let injected_authorization = resolve_addon_network_auth_header(
        &addon_id,
        request.auth.as_ref(),
        context.secret_store.as_ref(),
    )?;
    request.injected_authorization = injected_authorization;
    context
        .addon_service
        .addon_network_request(&addon_id, request)
        .await
}
