#include <stdio.h>
#include <ndds/ndds_c.h>


#define MAX_STRING_SIZE         1024

static void on_data_available_callback1(void*, DDS_DataReader*);
static void on_data_available_callback2(void*, DDS_DataReader*);
static void on_data_available_callback3(void*, DDS_DataReader*);

DDS_Boolean shutdown_flag = DDS_BOOLEAN_FALSE;

struct Entry {
    DDS_DomainParticipant *part;
    DDS_Topic             *t;
    DDS_Publisher         *p;
    DDS_Subscriber        *s;
    DDS_DataReader        *r;
    DDS_DataWriter        *w;
    DDS_KeyedStringDataWriter  *sw;
    struct DDS_DataReaderListener listener;
    DDS_InstanceHandle_t  handle;
};

int main() {
    struct Entry entries[3];
    char * start_partition[3] = {"A", "A", "A"};
    DDS_KeyedString PAYLOAD[3] = {{"1", "Hello from 1"}, {"2", "Hello from 2"}, {"3", "Hello from 3"}};
    struct DDS_DataReaderListener listeners[3] = { DDS_DataReaderListener_INITIALIZER, DDS_DataReaderListener_INITIALIZER, DDS_DataReaderListener_INITIALIZER };
    listeners[0].on_data_available = on_data_available_callback1;
    listeners[1].on_data_available = on_data_available_callback2;
    listeners[2].on_data_available = on_data_available_callback3;

    int entry_count = sizeof(entries) / sizeof(struct Entry);

    for(int i = 0; i < entry_count; i++) {
        struct DDS_DomainParticipantQos partQos = DDS_DomainParticipantQos_INITIALIZER;
        DDS_DomainParticipantFactory_get_default_participant_qos(DDS_TheParticipantFactory, &partQos);
        partQos.transport_builtin.mask = DDS_TRANSPORTBUILTIN_UDPv4;

        entries[i].part = DDS_DomainParticipantFactory_create_participant(
                            DDS_TheParticipantFactory, 0,
                            &partQos,
                            NULL, DDS_STATUS_MASK_NONE);
        entries[i].t = DDS_DomainParticipant_create_topic(
                        entries[i].part, "Hello, World", DDS_KeyedStringTypeSupport_get_type_name(), 
                        &DDS_TOPIC_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);

        struct DDS_SubscriberQos sQos = DDS_SubscriberQos_INITIALIZER;
        DDS_DomainParticipant_get_default_subscriber_qos(entries[i].part, &sQos);
        DDS_StringSeq_ensure_length(&sQos.partition.name, 1, 1);
        *DDS_StringSeq_get_reference(&sQos.partition.name, 0) = DDS_String_dup(start_partition[i]);
        entries[i].s = DDS_DomainParticipant_create_subscriber(entries[i].part, &sQos, NULL, DDS_STATUS_MASK_NONE);

        struct DDS_PublisherQos pQos = DDS_PublisherQos_INITIALIZER;
        DDS_DomainParticipant_get_default_publisher_qos(entries[i].part, &pQos);
        DDS_StringSeq_ensure_length(&pQos.partition.name, 1, 1);
        *DDS_StringSeq_get_reference(&pQos.partition.name, 0) = DDS_String_dup(start_partition[i]);
        entries[i].p = DDS_DomainParticipant_create_publisher(entries[i].part, &pQos, NULL, DDS_STATUS_MASK_NONE);

        struct DDS_DataReaderQos drQos = DDS_DataReaderQos_INITIALIZER;
        DDS_Subscriber_get_default_datareader_qos(entries[i].s, &drQos);
        drQos.liveliness.kind = DDS_AUTOMATIC_LIVELINESS_QOS;
        drQos.liveliness.lease_duration.sec = 1;
        drQos.liveliness.lease_duration.nanosec = 0;
        entries[i].r = DDS_Subscriber_create_datareader(entries[i].s, DDS_Topic_as_topicdescription(entries[i].t), &drQos, &listeners[i], DDS_DATA_AVAILABLE_STATUS);

        struct DDS_DataWriterQos dwQos = DDS_DataWriterQos_INITIALIZER;
        DDS_Publisher_get_default_datawriter_qos(entries[i].p, &dwQos);
        dwQos.liveliness.kind = DDS_AUTOMATIC_LIVELINESS_QOS;
        dwQos.liveliness.lease_duration.sec = 1;
        dwQos.liveliness.lease_duration.nanosec = 0;
        entries[i].w = DDS_Publisher_create_datawriter(entries[i].p, entries[i].t, &dwQos, NULL, DDS_STATUS_MASK_NONE);

        entries[i].sw = DDS_KeyedStringDataWriter_narrow(entries[i].w);
    }

#ifdef RTI_WIN32
        Sleep(2000);
#else
        sleep(2);
#endif

    puts("Ready to read data.");
    puts("Press CTRL+C to terminate.");
    // entries[0].handle = DDS_StringDataWriter_register_instance(entries[0].sw, "Hello from 1");
    // DDS_StringDataWriter_write(entries[0].sw, "Hello from 1", &entries[0].handle);



    // entries[2].handle = DDS_StringDataWriter_register_instance(entries[2].sw, "Hello from 3");
    // DDS_StringDataWriter_write(entries[2].sw, "Hello from 3", &entries[2].handle);    

#ifdef RTI_WIN32
        Sleep(1000);
#else
        sleep(1);
#endif

        entries[1].handle = DDS_KeyedStringDataWriter_register_instance(entries[1].sw, &PAYLOAD[1]);
        DDS_KeyedStringDataWriter_write(entries[1].sw, &PAYLOAD[1], &entries[1].handle);
// #ifdef RTI_WIN32
//         Sleep(1000);
// #else
//         sleep(1);
// #endif

// DDS_KeyedStringDataWriter_unregister_instance(entries[1].sw, &PAYLOAD[1], &entries[1].handle);

    struct DDS_PublisherQos pQos = DDS_PublisherQos_INITIALIZER;
    
    for (;;) {



        DDS_Publisher_get_qos(entries[1].p, &pQos);
        fprintf(stderr, "Partition was %s\n", DDS_StringSeq_get(&pQos.partition.name, 0));
        *DDS_StringSeq_get_reference(&pQos.partition.name, 0) = DDS_String_dup("A"); // DDS_String_dup(start_partition[i]);
        DDS_Publisher_set_qos(entries[1].p, &pQos);

#ifdef RTI_WIN32
        Sleep(1000);
#else
        sleep(1);
#endif        
        DDS_KeyedStringDataWriter_write(entries[1].sw, &PAYLOAD[1], &entries[1].handle);
        fprintf(stderr, "Changed to partition A\n");    

#ifdef RTI_WIN32
        Sleep(5000);
#else
        sleep(5);
#endif

        DDS_Publisher_get_qos(entries[1].p, &pQos);
        fprintf(stderr, "Partition was %s\n", DDS_StringSeq_get(&pQos.partition.name, 0));
        *DDS_StringSeq_get_reference(&pQos.partition.name, 0) = DDS_String_dup("B"); // DDS_String_dup(start_partition[i]);
        DDS_Publisher_set_qos(entries[1].p, &pQos);
        fprintf(stderr, "Changed to partition B\n");
#ifdef RTI_WIN32
        Sleep(5000);
#else
        sleep(5);
#endif
        
        if(shutdown_flag){
            break;
        }
    }

    puts("Exiting...");

    for(int i = 0; i < entry_count; i++) {
        DDS_DomainParticipant_delete_contained_entities(entries[i].part);
        DDS_DomainParticipantFactory_delete_participant(DDS_TheParticipantFactory, entries[i].part);
    }

}

static void on_data_available_callback(void *listener_data, 
                                       DDS_DataReader* data_reader,
                                       const char * identifier) {
    DDS_KeyedStringDataReader *string_reader = NULL;
    struct DDS_KeyedStringSeq samples = DDS_SEQUENCE_INITIALIZER;
    struct DDS_SampleInfoSeq infos = DDS_SEQUENCE_INITIALIZER;
    DDS_ReturnCode_t      retcode;

    string_reader = DDS_KeyedStringDataReader_narrow(data_reader);
    if (string_reader == NULL) {
        puts("DDS_KeyedStringDataReader_narrow failed.");
        return;
    }

    for(;;) {
        retcode = DDS_KeyedStringDataReader_read(
                            string_reader,
                            &samples,
                            &infos,
                            DDS_LENGTH_UNLIMITED,
                            DDS_NOT_READ_SAMPLE_STATE,
                            DDS_ANY_VIEW_STATE,
                            DDS_ANY_INSTANCE_STATE);
        if (retcode == DDS_RETCODE_NO_DATA) {
            break;
        } else if (retcode != DDS_RETCODE_OK) {
            printf("Unable to take data from data reader, error %d\n", retcode);
            return;
        }
        DDS_Long length = DDS_SampleInfoSeq_get_length(&infos);

        for(DDS_Long i = 0; i < length; i++) {
            struct DDS_SampleInfo *info = DDS_SampleInfoSeq_get_reference(&infos, i);
            DDS_KeyedString *sample = DDS_KeyedStringSeq_get_reference(&samples, i);
            if(!info->valid_data) {
                DDS_KeyedStringDataReader_get_key_value(string_reader, sample, &info->instance_handle);
            }
            fprintf(stderr, "%s: %s %s\n", identifier, sample->key, 0!=(info->instance_state&DDS_ALIVE_INSTANCE_STATE)?"ALIVE":"NOT ALIVE");
        }

        DDS_KeyedStringDataReader_return_loan(string_reader, &samples, &infos);
    }
}

static void on_data_available_callback1(void *listener_data, 
                                       DDS_DataReader* data_reader) {
    on_data_available_callback(listener_data, data_reader, "1");
}

static void on_data_available_callback2(void *listener_data, 
                                       DDS_DataReader* data_reader) {
    on_data_available_callback(listener_data, data_reader, "2");
}

static void on_data_available_callback3(void *listener_data, 
                                       DDS_DataReader* data_reader) {
    on_data_available_callback(listener_data, data_reader, "3");
}
