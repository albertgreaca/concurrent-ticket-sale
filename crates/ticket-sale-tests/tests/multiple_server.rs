use eyre::Result;
use ticket_sale_tests::{RequestOptions, Reservation, TestCtxBuilder};
use util::scale_to;

mod util;
#[tokio::test] // Every test function needs to be decorated with this attribute
#[ntest::timeout(20_000)] // Test timeout in ms
async fn test_buy_100_tickets_on_scaled_servers() -> Result<()> {
    let ctx = TestCtxBuilder::from_env()?
        .with_tickets(10_000)
        .build()
        .await?;
    let _ = scale_to(&ctx, 20).await;
    for _i in 1..1_001 {
        let mut session = ctx.api.create_user_session(None);
        match session.reserve_ticket().await?.result? {
            Reservation::SoldOut => {
                panic!("It must be possible to reserve a ticket.")
            }
            Reservation::Reserved(ticket_id) => {
                assert!(
                    session.buy_ticket(ticket_id).await?.result.is_ok(),
                    "It must be possible to buy the ticket that we just reserved.",
                );
            }
        }
    }

    let _ = scale_to(&ctx, 1).await;
    for _i in 1_001..10_001 {
        let mut session = ctx.api.create_user_session(None);
        match session.reserve_ticket().await?.result? {
            Reservation::SoldOut => {
                panic!("It must be possible to reserve a ticket.")
            }
            Reservation::Reserved(ticket_id) => {
                assert!(
                    session.buy_ticket(ticket_id).await?.result.is_ok(),
                    "It must be possible to buy the ticket that we just reserved.",
                );
            }
        }
    }
    let _ = scale_to(&ctx, 100).await;
    // Finish the test
    ctx.finish().await;
    Ok(())
}
