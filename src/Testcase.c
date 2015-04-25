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
    DDS_StringDataWriter  *sw;
    struct DDS_DataReaderListener listener;
};

int main() {
    struct Entry entries[3];
    char * start_partition[3] = {"A", "A", "*"};
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
                        entries[i].part, "Hello, World", DDS_StringTypeSupport_get_type_name(), 
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

        entries[i].r = DDS_Subscriber_create_datareader(entries[i].s, DDS_Topic_as_topicdescription(entries[i].t), &DDS_DATAREADER_QOS_DEFAULT, &listeners[i], DDS_DATA_AVAILABLE_STATUS);
        entries[i].w = DDS_Publisher_create_datawriter(entries[i].p, entries[i].t, &DDS_DATAWRITER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);
        entries[i].sw = DDS_StringDataWriter_narrow(entries[i].w);
    }

    
    /* --- Sleep During Asynchronous Reception ---------------------------- */

    /* This thread sleeps forever. When a sample is received, RTI Data
     * Distribution Service will call the on_data_available_callback function.
     */
    puts("Ready to read data.");
    puts("Press CTRL+C to terminate.");
    for (;;) {
        DDS_StringDataWriter_write(entries[0].sw, "Hello", &DDS_HANDLE_NIL);
#ifdef RTI_WIN32
        Sleep(2000);
#else
        sleep(2);
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

/* on_data_available_callback
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~
 * This is the callback function that is invoked by RTI Connext whenever a new sample is received.
 */
static void on_data_available_callback(void *listener_data /* unused */, 
                                       DDS_DataReader* data_reader,
                                       const char * identifier) {
    DDS_StringDataReader *string_reader = NULL;
    struct DDS_StringSeq samples = DDS_SEQUENCE_INITIALIZER;
    struct DDS_SampleInfoSeq infos = DDS_SEQUENCE_INITIALIZER;
    DDS_ReturnCode_t      retcode;

    /* Perform a safe type-cast from a generic data reader into a
     * specific data reader for the type "DDS::String"
     */
    string_reader = DDS_StringDataReader_narrow(data_reader);
    if (string_reader == NULL) {
        /* In this specific case, this will never fail */
        puts("DDS_StringDataReader_narrow failed.");
        return;
    }
    /* Loop until there are messages available in the queue */
    for(;;) {
        retcode = DDS_StringDataReader_read(
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
            struct DDS_SampleInfo info = DDS_SampleInfoSeq_get(&infos, i);
            char * sample = DDS_StringSeq_get(&samples, i);
            fprintf(stderr, "%s: %s %s\n", identifier, sample, 0!=(info.instance_state&DDS_ALIVE_INSTANCE_STATE)?"ALIVE":"NOT ALIVE");
            // TODO CHECK FOR LIVELINESS AND REPORT TO CONSOLE
        }

        DDS_StringDataReader_return_loan(string_reader, &samples, &infos);
    }
}

static void on_data_available_callback1(void *listener_data /* unused */, 
                                       DDS_DataReader* data_reader) {
    on_data_available_callback(listener_data, data_reader, "1");
}

static void on_data_available_callback2(void *listener_data /* unused */, 
                                       DDS_DataReader* data_reader) {
    on_data_available_callback(listener_data, data_reader, "2");
}

static void on_data_available_callback3(void *listener_data /* unused */, 
                                       DDS_DataReader* data_reader) {
    on_data_available_callback(listener_data, data_reader, "3");
}
