package net.messaging.clusterbox;

import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.messaging.clusterbox.message.Message;
import net.messaging.clusterbox.message.RequestMessage;

public class MessageTest {

    private static class MyPayload {
        private String firstName;
        private Integer age;
        private String lastName;

        public MyPayload() {

        }

        public MyPayload(String firstName, Integer age, String lastName) {
            super();
            this.firstName = firstName;
            this.age = age;
            this.lastName = lastName;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        @Override
        public String toString() {
            return "TestPayload [firstName=" + firstName + ", age=" + age + ", lastName=" + lastName + "]";
        }

    }

    @Test
    public void testSerializationAndDeserializationOfMessage() throws Exception {
        MyPayload payload = new MyPayload("firstName", 10, "LastName");
        RequestMessage<MyPayload> message = new RequestMessage<MyPayload>(payload);
        ObjectMapper mapper = new ObjectMapper();
        String string = mapper.writeValueAsString(message);
        System.out.println(string);
        Message object = mapper.readValue(string, RequestMessage.class);
        MyPayload x = (MyPayload) object.getPayload();
        System.out.println(x);
    }

}
