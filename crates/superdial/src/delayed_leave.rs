//! The loop that keeps a delayed leave from firing while the client is alive.

use std::{sync::Arc, time::Duration};

use tokio::{sync::oneshot, task::JoinHandle, time::MissedTickBehavior};

use crate::{
    homeserver::{DelayedAction, Homeserver},
    Error,
};

/// Restarts a delayed event. Implemented by [`Homeserver`]; a fake in tests.
#[async_trait::async_trait]
pub trait DelayedEvents: Send + Sync {
    async fn restart(&self, delay_id: &str) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl DelayedEvents for Homeserver {
    async fn restart(&self, delay_id: &str) -> Result<(), Error> {
        self.delayed_action(delay_id, DelayedAction::Restart).await
    }
}

/// A running restart loop. Dropping it without [`RestartLoop::stop`] aborts it.
pub struct RestartLoop {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

/// Restarts `delay_id` every `every`, the first time one interval from now. A failed restart is
/// logged and the loop goes on: one network blip must not let the leave fire.
pub fn spawn_restart_loop(api: Arc<dyn DelayedEvents>, delay_id: String, every: Duration) -> RestartLoop {
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = tick.tick() => {
                    if let Err(e) = api.restart(&delay_id).await {
                        tracing::warn!(%delay_id, "restarting the delayed leave failed: {e}");
                    }
                }
            }
        }
    });
    RestartLoop {
        stop: Some(stop_tx),
        task: Some(task),
    }
}

impl RestartLoop {
    /// Stops the loop and waits for it to end.
    pub async fn stop(mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for RestartLoop {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<String>>,
        fail_first: Mutex<u32>,
    }

    #[async_trait::async_trait]
    impl DelayedEvents for Recorder {
        async fn restart(&self, delay_id: &str) -> Result<(), Error> {
            self.calls.lock().unwrap().push(delay_id.to_owned());
            let mut f = self.fail_first.lock().unwrap();
            if *f > 0 {
                *f -= 1;
                return Err(Error::Transport("blip".into()));
            }
            Ok(())
        }
    }

    impl Recorder {
        fn count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    async fn advance(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn no_restart_before_first_interval() {
        let rec = Arc::new(Recorder::default());
        let lp = spawn_restart_loop(rec.clone(), "d1".into(), Duration::from_secs(5));
        advance(4900).await;
        assert_eq!(rec.count(), 0);
        lp.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn restarts_every_five_seconds() {
        let rec = Arc::new(Recorder::default());
        let lp = spawn_restart_loop(rec.clone(), "d1".into(), Duration::from_secs(5));
        advance(5100).await;
        assert_eq!(rec.count(), 1);
        advance(10_000).await;
        assert_eq!(rec.count(), 3);
        assert!(rec.calls.lock().unwrap().iter().all(|id| id == "d1"));
        lp.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn stop_ends_restarts() {
        let rec = Arc::new(Recorder::default());
        let lp = spawn_restart_loop(rec.clone(), "d1".into(), Duration::from_secs(5));
        advance(5100).await;
        lp.stop().await;
        advance(30_000).await;
        assert_eq!(rec.count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_restart_does_not_stop_loop() {
        let rec = Arc::new(Recorder {
            fail_first: Mutex::new(2),
            ..Default::default()
        });
        let lp = spawn_restart_loop(rec.clone(), "d1".into(), Duration::from_secs(5));
        advance(15_100).await;
        assert_eq!(rec.count(), 3);
        lp.stop().await;
    }
}
