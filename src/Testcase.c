#include <stdio.h>
#include <ndds/ndds_c.h>


#define MAX_STRING_SIZE         1024

static void on_data_available_callback(void*, DDS_DataReader*);

DDS_Boolean shutdown_flag = DDS_BOOLEAN_FALSE;

int main() {
    DDS_DomainParticipant *       part1 = NULL;
    DDS_DomainParticipant *       part2 = NULL;
    DDS_DomainParticipant *       part3 = NULL;
    DDS_Topic *                   t1 = NULL;
    DDS_Topic *                   t2 = NULL;
    DDS_Topic *                   t3 = NULL;
    DDS_Publisher *               p1 = NULL;
    DDS_Publisher *               p2 = NULL; 
    DDS_Publisher *               p3 = NULL;
    DDS_Subscriber *              s1 = NULL;
    DDS_Subscriber *              s2 = NULL;
    DDS_Subscriber *              s3 = NULL;
    DDS_DataReader *              r1 = NULL;
    DDS_DataReader *              r2 = NULL;
    DDS_DataReader *              r3 = NULL;
    DDS_DataWriter *              w1 = NULL;
    DDS_DataWriter *              w2 = NULL;
    DDS_DataWriter *              w3 = NULL;

    struct DDS_DataReaderListener listener = DDS_DataReaderListener_INITIALIZER;
    DDS_ReturnCode_t              retcode;
    int                           main_result = 1; /* error by default */

    part1 = DDS_DomainParticipantFactory_create_participant(
                        DDS_TheParticipantFactory,
                        0,                              /* Domain ID */
                        &DDS_PARTICIPANT_QOS_DEFAULT,   /* QoS */
                        NULL,                           /* Listener */
                        DDS_STATUS_MASK_NONE);
    if (part1 == NULL) {
        puts("Unable to create domain participant 1.");
        goto clean_exit;
    }

    part2 = DDS_DomainParticipantFactory_create_participant(
                        DDS_TheParticipantFactory,
                        0,                              /* Domain ID */
                        &DDS_PARTICIPANT_QOS_DEFAULT,   /* QoS */
                        NULL,                           /* Listener */
                        DDS_STATUS_MASK_NONE);
    if (part2 == NULL) {
        puts("Unable to create domain participant 2.");
        goto clean_exit;
    }    

    part3 = DDS_DomainParticipantFactory_create_participant(
                        DDS_TheParticipantFactory,
                        0,                              /* Domain ID */
                        &DDS_PARTICIPANT_QOS_DEFAULT,   /* QoS */
                        NULL,                           /* Listener */
                        DDS_STATUS_MASK_NONE);
    if (part3 == NULL) {
        puts("Unable to create domain participant 3.");
        goto clean_exit;
    }    

    t1 = DDS_DomainParticipant_create_topic(
                        part1, 
                        "Hello, World",                        /* Topic name*/
                        DDS_StringTypeSupport_get_type_name(), /* Type name */
                        &DDS_TOPIC_QOS_DEFAULT,                /* Topic QoS */
                        NULL,                                  /* Listener  */
                        DDS_STATUS_MASK_NONE);
    if (t1 == NULL) {
        puts("Unable to create topic1.");
        goto clean_exit;
    }

    t2 = DDS_DomainParticipant_create_topic(
                        part2, 
                        "Hello, World",                        /* Topic name*/
                        DDS_StringTypeSupport_get_type_name(), /* Type name */
                        &DDS_TOPIC_QOS_DEFAULT,                /* Topic QoS */
                        NULL,                                  /* Listener  */
                        DDS_STATUS_MASK_NONE);
    if (t2 == NULL) {
        puts("Unable to create topic2.");
        goto clean_exit;
    }    

    t3 = DDS_DomainParticipant_create_topic(
                        part3, 
                        "Hello, World",                        /* Topic name*/
                        DDS_StringTypeSupport_get_type_name(), /* Type name */
                        &DDS_TOPIC_QOS_DEFAULT,                /* Topic QoS */
                        NULL,                                  /* Listener  */
                        DDS_STATUS_MASK_NONE);
    if (t3 == NULL) {
        puts("Unable to create topic3.");
        goto clean_exit;
    }    
    struct DDS_SubscriberQos sQos;
    DDS_DomainParticipant_get_default_subscriber_qos(part1, &sQos);
    DDS_StringSeq_ensure_length(&sQos.partition.name, 1, 1);
    *DDS_StringSeq_get_reference(&sQos.partition.name, 0) = DDS_String_dup("A");

    s1 = DDS_DomainParticipant_create_subscriber(part1, &sQos, NULL, DDS_STATUS_MASK_NONE);
    if(!s1) {
        puts("Unable to create s1");
        goto clean_exit;
    }
    DDS_DomainParticipant_get_default_subscriber_qos(part2, &sQos);
    DDS_StringSeq_ensure_length(&sQos.partition.name, 1, 1);
    *DDS_StringSeq_get_reference(&sQos.partition.name, 0) = DDS_String_dup("A");
    s2 = DDS_DomainParticipant_create_subscriber(part2, &sQos, NULL, DDS_STATUS_MASK_NONE);
    if(!s2) {
        puts("Unable to create s2");
        goto clean_exit;
    }
    DDS_DomainParticipant_get_default_subscriber_qos(part3, &sQos);
    DDS_StringSeq_ensure_length(&sQos.partition.name, 1, 1);
    *DDS_StringSeq_get_reference(&sQos.partition.name, 0) = DDS_String_dup("*");
    s3 = DDS_DomainParticipant_create_subscriber(part3, &sQos, NULL, DDS_STATUS_MASK_NONE);
    if(!s3) {
        puts("Unable to create s3");
        goto clean_exit;
    }   
    struct DDS_PublisherQos pQos;
    DDS_DomainParticipant_get_default_publisher_qos(part1, &pQos);
    DDS_StringSeq_ensure_length(&pQos.partition.name, 1, 1);
    *DDS_StringSeq_get_reference(&pQos.partition.name, 0) = DDS_String_dup("A");
    p1 = DDS_DomainParticipant_create_publisher(part1, &pQos, NULL, DDS_STATUS_MASK_NONE);
    if(!p1) {
        puts("Unable to create p1");
        goto clean_exit;
    }
    DDS_DomainParticipant_get_default_publisher_qos(part2, &pQos);
    DDS_StringSeq_ensure_length(&pQos.partition.name, 1, 1);
    *DDS_StringSeq_get_reference(&pQos.partition.name, 0) = DDS_String_dup("A");    
    p2 = DDS_DomainParticipant_create_publisher(part2, &pQos, NULL, DDS_STATUS_MASK_NONE);
    if(!p2) {
        puts("Unable to create p2");
        goto clean_exit;
    }
    DDS_DomainParticipant_get_default_publisher_qos(part3, &pQos);
    DDS_StringSeq_ensure_length(&pQos.partition.name, 1, 1);
    *DDS_StringSeq_get_reference(&pQos.partition.name, 0) = DDS_String_dup("*");
    p3 = DDS_DomainParticipant_create_publisher(part3, &pQos, NULL, DDS_STATUS_MASK_NONE);
    if(!p3) {
        puts("Unable to create p3");
        goto clean_exit;
    }    

    r1 = DDS_Subscriber_create_datareader(s1, DDS_Topic_as_topicdescription(t1), &DDS_DATAREADER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);
    r2 = DDS_Subscriber_create_datareader(s2, DDS_Topic_as_topicdescription(t2), &DDS_DATAREADER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);
    r3 = DDS_Subscriber_create_datareader(s3, DDS_Topic_as_topicdescription(t3), &DDS_DATAREADER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);

    w1 = DDS_Publisher_create_datawriter(p1, t1, &DDS_DATAWRITER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);
    w2 = DDS_Publisher_create_datawriter(p2, t2, &DDS_DATAWRITER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);
    w3 = DDS_Publisher_create_datawriter(p3, t3, &DDS_DATAWRITER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);

    /* Create the data reader using the default publisher */
    listener.on_data_available = on_data_available_callback;
    r1 = DDS_Subscriber_create_datareader(
                        s1,
                        DDS_Topic_as_topicdescription(t1),
                        &DDS_DATAREADER_QOS_DEFAULT,    /* QoS */
                        &listener,                      /* Listener */
                        DDS_DATA_AVAILABLE_STATUS);
    if (r1 == NULL) {
        puts("Unable to create data reader.");
        goto clean_exit;
    }
    
    /* --- Sleep During Asynchronous Reception ---------------------------- */

    /* This thread sleeps forever. When a sample is received, RTI Data
     * Distribution Service will call the on_data_available_callback function.
     */
    puts("Ready to read data.");
    puts("Press CTRL+C to terminate.");
    for (;;) {
#ifdef RTI_WIN32
        Sleep(2000);
#else
        sleep(2);
#endif
        if(shutdown_flag){
            break;
        }
    }
    
    /* --- Clean Up ------------------------------------------------------- */

    main_result = 0;
clean_exit:
    puts("Exiting...");

    if (part1 != NULL) {
        retcode = DDS_DomainParticipant_delete_contained_entities(
                        part1);
        if (retcode != DDS_RETCODE_OK) {
            puts("Deletion failed.");
            main_result = 1;
        }
        retcode = DDS_DomainParticipantFactory_delete_participant(
                        DDS_TheParticipantFactory,
                        part1);
        if (retcode != DDS_RETCODE_OK) {
            puts("Deletion failed.");
            main_result = 1;
        }
    } 
    if (part2 != NULL) {
        retcode = DDS_DomainParticipant_delete_contained_entities(
                        part2);
        if (retcode != DDS_RETCODE_OK) {
            puts("Deletion failed.");
            main_result = 1;
        }
        retcode = DDS_DomainParticipantFactory_delete_participant(
                        DDS_TheParticipantFactory,
                        part2);
        if (retcode != DDS_RETCODE_OK) {
            puts("Deletion failed.");
            main_result = 1;
        }
    }     
    if (part3 != NULL) {
        retcode = DDS_DomainParticipant_delete_contained_entities(
                        part3);
        if (retcode != DDS_RETCODE_OK) {
            puts("Deletion failed.");
            main_result = 1;
        }
        retcode = DDS_DomainParticipantFactory_delete_participant(
                        DDS_TheParticipantFactory,
                        part3);
        if (retcode != DDS_RETCODE_OK) {
            puts("Deletion failed.");
            main_result = 1;
        }
    }     
    return main_result;
}

/* on_data_available_callback
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~
 * This is the callback function that is invoked by RTI Connext whenever a new sample is received.
 */
static void on_data_available_callback(void *listener_data /* unused */, 
                                       DDS_DataReader* data_reader) {
    DDS_StringDataReader *string_reader = NULL;
    char                  sample[MAX_STRING_SIZE]; 
    struct DDS_SampleInfo info;
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
        retcode = DDS_StringDataReader_take_next_sample(
                            string_reader,
                            sample,
                            &info);
        if (retcode == DDS_RETCODE_NO_DATA) {
            /* No more samples */
            break;
        } else if (retcode != DDS_RETCODE_OK) {
            printf("Unable to take data from data reader, error %d\n", retcode);
            return;
        }
        if (info.valid_data) {
            /* Data is valid (this isn't just a lifecycle sample): print it */
            puts(sample);
            /* If empty string is received, clean shutdown*/
            if(strlen(sample) == 0){
                shutdown_flag = DDS_BOOLEAN_TRUE;	
            }
        }
    }
}



