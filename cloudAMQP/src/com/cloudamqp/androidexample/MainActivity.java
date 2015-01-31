package com.cloudamqp.androidexample;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import com.cloudamqp.androidexample.R;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;

import android.app.Activity;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class MainActivity extends Activity {
	
	ConnectionFactory factory = new ConnectionFactory();
	private BlockingDeque<String> queue = new LinkedBlockingDeque<String>();
	private static final String EXCHANGE_NAME = "diseases";
	LinkedList<String> bindings =  new LinkedList<>();
	Thread subscribeThread;
	Thread publishThread;

	@Override
	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_main);
		addTopicsToBindings();
		setupConnectionFactory();
		publishToAMQP();
		setupPubButton();
		final Handler incomingMessageHandler = new Handler() {
			@Override
			public void handleMessage(Message msg) {
				String message = msg.getData().getString("msg");
				TextView tv = (TextView) findViewById(R.id.textView);
				Date now = new Date();
				SimpleDateFormat ft = new SimpleDateFormat("hh:mm:ss");
				tv.append(ft.format(now) + ' ' + message + '\n');
			}
		};
		subscribe(incomingMessageHandler);
	}
	
	void addTopicsToBindings() {
		bindings.add("ebola"); bindings.add("sars"); bindings.add("swine");
	}
	
	String getTopic(String entry) {
		int index = entry.indexOf(' ');
		String topic = entry.substring(0, index);
		return topic;
	}
	void setupPubButton() {
		Button button = (Button) findViewById(R.id.publish);
		button.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View arg0) {
				EditText et = (EditText) findViewById(R.id.text);
				EditText et2 = (EditText) findViewById(R.id.editText1);

				publishMessage(et.getText().toString(), et2.getText().toString());
				et.setText("");
				et2.setText("");
			}
		});
	}


	@Override
	protected void onDestroy() {
		super.onDestroy();
		publishThread.interrupt();
		subscribeThread.interrupt();
	}
	

	void publishMessage(String message, String topic) {
		// Adds a message to internal blocking queue
		try {
			Log.d("", "[q] " + message);
			String entry = topic + " " + message;
			queue.putLast(entry);
			System.out.println("topic is" + getTopic(entry));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	private void setupConnectionFactory() {
		
		String uri = "amqp://uteaclum:EwuM_y__CYoRlNaqRqKxEcqRbkOAmbkk@turtle.rmq.cloudamqp.com/uteaclum";
		try {
			factory.setAutomaticRecoveryEnabled(false);
			factory.setUri(uri);
		} catch (KeyManagementException | NoSuchAlgorithmException
				| URISyntaxException e1) {
			e1.printStackTrace();
		}
	}

	void subscribe(final Handler handler) {
		subscribeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						System.out.println("into subscriber");
						Connection connection = factory.newConnection();
						Channel channel = connection.createChannel();
						//adding code for topics
			     		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
						String queueName = channel.queueDeclare().getQueue();
						System.out.println("queueName is " + queueName);
//						if (argv.length < 1) {
//							System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
//							System.exit(1);
//						}
						for (String bindingKey : bindings) {
							channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
						}
						QueueingConsumer consumer = new QueueingConsumer(channel);
						channel.basicConsume(queueName, true, consumer);
						while (true) {
							System.out.println("entered the dreaded loop");
							QueueingConsumer.Delivery delivery = consumer.nextDelivery();
							String message = new String(delivery.getBody());
							Log.d("", "[r] " + message);
							System.out.println("subscriber message" + message);
							Bundle bundle = new Bundle();
							bundle.putString("msg", message);
							Message msg = handler.obtainMessage();
							msg.setData(bundle);
							handler.sendMessage(msg);
							System.out.println("left the dreaded loop");

//							String routingKey = delivery.getEnvelope().getRoutingKey();
//							System.out.println(" [x] Received '" + routingKey + "':'"
//									+ message + "'");
						}
						
//						channel.basicQos(1);
//						DeclareOk q = channel.queueDeclare();
//						channel.queueBind(q.getQueue(), "amq.fanout", "chat");
//						QueueingConsumer consumer = new QueueingConsumer(
//								channel);
//						channel.basicConsume(q.getQueue(), true, consumer);
						// Process deliveries
//						while (true) {
//							QueueingConsumer.Delivery delivery = consumer.nextDelivery();
//							String message = new String(delivery.getBody());
//							Log.d("", "[r] " + message);
//							Message msg = handler.obtainMessage();
//							Bundle bundle = new Bundle();
//							bundle.putString("msg", message);
//							msg.setData(bundle);
//							handler.sendMessage(msg);
//						}
					} catch (InterruptedException e) {
						break;
					} catch (Exception e1) {
						Log.d("", "Connection broken: "
								+ e1.getClass().getName());
						try {
							Thread.sleep(4000); // sleep and then try again
						} catch (InterruptedException e) {
							break;
						}
					}
				}
			}
		});
		subscribeThread.start();
	}

	public void publishToAMQP() {
		publishThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						System.out.println("into publisher");
						Connection connection = factory.newConnection();
						Channel ch = connection.createChannel();
						//adding code
						ch.exchangeDeclare(EXCHANGE_NAME, "topic");
						ch.confirmSelect();
						while (true) {
							String message = queue.takeFirst();
						
							try {
								System.out.println("routing key" + getTopic(message));
								ch.basicPublish(EXCHANGE_NAME, getTopic(message), null,
										message.getBytes());
//								ch.basicPublish("amq.fanout", "chat", null,
//										message.getBytes());
								Log.d("", "[s] " + message);
								ch.waitForConfirmsOrDie();
								System.out.println("published");
							} catch (Exception e) {
								Log.d("", "[f] " + message);
								queue.putFirst(message);
								throw e;
							}
						}
					} catch (InterruptedException e) {
						break;
					} catch (Exception e) {
						Log.d("", "Connection broken: "
								+ e.getClass().getName());
						try {
							Thread.sleep(5000); // sleep and then try again
						} catch (InterruptedException e1) {
							break;
						}
					}
				}
			}
		});
		publishThread.start();
	}
}