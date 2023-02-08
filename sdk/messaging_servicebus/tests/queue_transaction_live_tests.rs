#![cfg(all(test, feature = "test_e2e"))]

#[macro_use]
mod macros;

cfg_transaction! {
    use azure_messaging_servicebus::prelude::*;

    mod common;
    use common::setup_dotenv;

    #[tokio::test]
    async fn run_queue_transaction_tests_sequentially() {
        let mut all_result: Result<(), anyhow::Error> = Ok(());

        print!("test commit_single_send ... ");
        let result = commit_single_send().await;
        println!(" ... {:?}", result);
        all_result = all_result.and(result);
    }


    async fn commit_single_send() -> Result<(), anyhow::Error> {
        setup_dotenv();
        let connection_string = std::env::var("SERVICE_BUS_CONNECTION_STRING")?;
        let queue_name = std::env::var("SERVICE_BUS_QUEUE")?;

        let mut client = ServiceBusClient::new(connection_string, Default::default()).await?;
        let mut sender = client.create_sender(queue_name, Default::default()).await?;

        client.transaction(|txn_scope| async {
            txn_scope.send_message(&mut sender, "hello world txn").await?;
            Ok(())
        }).await;

        sender.dispose().await?;
        client.dispose().await?;
        Ok(())
    }
}
