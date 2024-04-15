CREATE TABLE public.subscribers_feedback (
    id bigserial NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start timestamp NOT NULL,
    adv_campaign_datetime_end timestamp NOT NULL,
    datetime_created timestamp NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created timestamp NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);
