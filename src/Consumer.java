import org.apache.activemq.*;

import javax.jms.*;
import javax.jms.Message;
import java.util.Date;

public class Consumer {
    private static String url = "tcp://localhost:61616"; //ActiveMQConnection.DEFAULT_BROKER_URL;
    private static String subject = "TESTQUEUE";

    public static void main(String[] args) throws Exception {
        new Producer().produce();
        for (int i = 0; i < 2; i++) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        new Consumer().consume();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }
        Thread.sleep(5 * 60000);
    }


    public void consume() throws Exception {
        ActiveMQConnection connection = null;
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
            ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
            prefetchPolicy.setQueuePrefetch(1);
            connectionFactory.setPrefetchPolicy(prefetchPolicy);
            final RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
            redeliveryPolicy.setRedeliveryDelay(1000);
            redeliveryPolicy.setMaximumRedeliveries(5);
            connectionFactory.setRedeliveryPolicy(redeliveryPolicy);
            connection = (ActiveMQConnection) connectionFactory.createConnection();
            connection.start();
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue(subject);
            final ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message msg) {
                    try {
                        final TextMessage message = (TextMessage) msg;
                        String sout = consumer.getConsumerName() +
                                " | " + Thread.currentThread().getName()
                                + " | Received " + message.getText() + " [" + message.getJMSMessageID()
                                + "] " + message.getJMSRedelivered();
                        if (message.getText().contains("Message 0")) {
                            System.err.println(sout + " | " + new Date());
                        } else {
                            System.out.println(sout);
                        }
                        handleMessage(message);
                        session.commit();

                    } catch (Exception e) {
                        try {
                            session.rollback();
                        } catch (JMSException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            });
            Thread.sleep(60000);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    private static void handleMessage(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            if (((TextMessage) message).getText().contains("Message 0")) {
                throw new RuntimeException("Failing jms message");
            }
        }
    }
}