//! Share user-interaction pauses with command observation windows.

use std::sync::Arc;

use crate::mcp::elicitation::ElicitationPauseState;
use crate::types::abort::AbortSignal;
use crate::types::permission::{ApprovalHandler, ApprovalRequest, ApprovalResponse};
use crate::types::tool::{ElicitHandler, ElicitRequest, ElicitResponse};

struct PausingElicit {
    inner: Arc<dyn ElicitHandler>,
    pause: ElicitationPauseState,
}

#[async_trait::async_trait]
impl ElicitHandler for PausingElicit {
    async fn elicit(&self, request: ElicitRequest, signal: AbortSignal) -> ElicitResponse {
        let _pause = self.pause.enter();
        self.inner.elicit(request, signal).await
    }
}

struct PausingApproval {
    inner: Arc<dyn ApprovalHandler>,
    pause: ElicitationPauseState,
}

#[async_trait::async_trait]
impl ApprovalHandler for PausingApproval {
    async fn approve(&self, request: ApprovalRequest, signal: AbortSignal) -> ApprovalResponse {
        let _pause = self.pause.enter();
        self.inner.approve(request, signal).await
    }
}

pub(crate) fn elicit(
    inner: Arc<dyn ElicitHandler>,
    pause: ElicitationPauseState,
) -> Arc<dyn ElicitHandler> {
    Arc::new(PausingElicit { inner, pause })
}

pub(crate) fn approval(
    inner: Arc<dyn ApprovalHandler>,
    pause: ElicitationPauseState,
) -> Arc<dyn ApprovalHandler> {
    Arc::new(PausingApproval { inner, pause })
}
