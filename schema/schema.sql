-- Complete current structure of train_delay_db. GENERATED - do not hand-edit.
--
-- WHY THIS FILE EXISTS. Until 2026-08-22 this repo had no CREATE TABLE for
-- any of the six core tables (countries, stations, routes, trips, stop_times,
-- delay_observations). They were created ad-hoc in psql when the project
-- started and never captured, which is why the numbered migrations begin at
-- 002 - there is no 001. The practical consequence: the repo alone could not
-- rebuild this database. Losing the box AND the dump meant losing the
-- structure too, not just the data. Found while making the project
-- restorable from GitHub; see RESTORE.md.
--
-- RELATIONSHIP TO schema/0NN_*.sql. Those are the historical record of each
-- change and why it was made - they are worth reading and are NOT superseded.
-- This file is the flattened result of all of them, so a restore applies THIS
-- one and does not replay the numbered migrations on top.
--
-- REGENERATE after any schema change:
--   sudo -u postgres pg_dump -s --no-owner train_delay_db > schema/schema.sql
--   (then re-add this header)
--
-- Grants are included deliberately: delay_observations and friends are owned
-- by the postgres superuser, not trainuser, so a restore that skips the GRANT
-- statements comes up with an app that cannot write. See NOTES.md, "Grants".
--
-- PostgreSQL database dump
--

\restrict Xkdj0joJolN3mkPg2I8pxfTTpQz0oQi5PvdEFkk5xEdop6qjIDdrVYH057xU7NJ

-- Dumped from database version 18.6 (Ubuntu 18.6-0ubuntu0.26.04.1)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-0ubuntu0.26.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: compensation_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.compensation_rules (
    id integer NOT NULL,
    country_code text NOT NULL,
    min_delay_minutes integer NOT NULL,
    refund_pct numeric(5,2) NOT NULL,
    source_url text,
    verified boolean DEFAULT false NOT NULL,
    notes text
);


--
-- Name: compensation_rules_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.compensation_rules_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: compensation_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.compensation_rules_id_seq OWNED BY public.compensation_rules.id;


--
-- Name: countries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.countries (
    code text NOT NULL,
    name text NOT NULL,
    CONSTRAINT countries_code_check CHECK (((code = upper(code)) AND (length(code) = 2)))
);


--
-- Name: delay_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.delay_observations (
    id integer NOT NULL,
    trip_id integer,
    station_id integer,
    observed_date date,
    actual_arrival timestamp without time zone,
    delay_minutes double precision,
    is_cancelled boolean,
    route_name text,
    country_code text DEFAULT 'HU'::text NOT NULL,
    headsign text,
    lat double precision,
    lon double precision,
    position_source text,
    live_trip_id text,
    platform text,
    cancellation_kind text,
    stop_sequence integer,
    scheduled_arrival text,
    scheduled_departure text,
    delay_arrival_minutes double precision,
    delay_departure_minutes double precision
);


--
-- Name: delay_observations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.delay_observations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: delay_observations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.delay_observations_id_seq OWNED BY public.delay_observations.id;


--
-- Name: outbound_clicks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outbound_clicks (
    id integer NOT NULL,
    partner_link_id integer NOT NULL,
    user_id integer,
    referer_path text,
    clicked_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: outbound_clicks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.outbound_clicks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: outbound_clicks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.outbound_clicks_id_seq OWNED BY public.outbound_clicks.id;


--
-- Name: partner_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.partner_links (
    id integer NOT NULL,
    country_code text,
    scope text NOT NULL,
    scope_key text,
    kind text NOT NULL,
    label text NOT NULL,
    target_url text NOT NULL,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: partner_links_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.partner_links_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: partner_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.partner_links_id_seq OWNED BY public.partner_links.id;


--
-- Name: routes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.routes (
    id integer NOT NULL,
    name text,
    gtfs_route_id text,
    route_type text,
    country_code text DEFAULT 'HU'::text NOT NULL,
    agency_name text,
    long_name text
);


--
-- Name: routes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.routes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: routes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.routes_id_seq OWNED BY public.routes.id;


--
-- Name: saved_trains; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.saved_trains (
    id integer NOT NULL,
    user_id integer NOT NULL,
    country_code text NOT NULL,
    identifier text NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: saved_trains_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.saved_trains_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: saved_trains_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.saved_trains_id_seq OWNED BY public.saved_trains.id;


--
-- Name: search_index; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.search_index (
    id integer NOT NULL,
    country_code text NOT NULL,
    kind text NOT NULL,
    name text NOT NULL,
    display_name text,
    observations bigint DEFAULT 0 NOT NULL,
    refreshed_at timestamp without time zone DEFAULT now() NOT NULL,
    search_text text DEFAULT ''::text NOT NULL
);


--
-- Name: search_index_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.search_index_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: search_index_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.search_index_id_seq OWNED BY public.search_index.id;


--
-- Name: service_calendar; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.service_calendar (
    id integer NOT NULL,
    country_code text NOT NULL,
    service_id text NOT NULL,
    monday boolean DEFAULT false NOT NULL,
    tuesday boolean DEFAULT false NOT NULL,
    wednesday boolean DEFAULT false NOT NULL,
    thursday boolean DEFAULT false NOT NULL,
    friday boolean DEFAULT false NOT NULL,
    saturday boolean DEFAULT false NOT NULL,
    sunday boolean DEFAULT false NOT NULL,
    start_date date,
    end_date date
);


--
-- Name: service_calendar_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.service_calendar_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: service_calendar_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.service_calendar_id_seq OWNED BY public.service_calendar.id;


--
-- Name: stations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stations (
    id integer NOT NULL,
    name text NOT NULL,
    gtfs_stop_id text,
    lat double precision,
    lon double precision,
    country_code text DEFAULT 'HU'::text NOT NULL,
    display_name text
);


--
-- Name: stations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stations_id_seq OWNED BY public.stations.id;


--
-- Name: stop_times; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stop_times (
    id integer NOT NULL,
    trip_id integer,
    station_id integer,
    scheduled_arrival text,
    scheduled_departure text,
    stop_sequence integer
);


--
-- Name: stop_times_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stop_times_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stop_times_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stop_times_id_seq OWNED BY public.stop_times.id;


--
-- Name: trips; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trips (
    id integer NOT NULL,
    route_id integer,
    gtfs_trip_id text,
    country_code text DEFAULT 'HU'::text NOT NULL,
    headsign text,
    service_id text,
    trip_short_name text
);


--
-- Name: trips_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.trips_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: trips_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.trips_id_seq OWNED BY public.trips.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    password_hash text NOT NULL,
    tier text DEFAULT 'free'::text NOT NULL,
    stripe_customer_id text,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: compensation_rules id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.compensation_rules ALTER COLUMN id SET DEFAULT nextval('public.compensation_rules_id_seq'::regclass);


--
-- Name: delay_observations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delay_observations ALTER COLUMN id SET DEFAULT nextval('public.delay_observations_id_seq'::regclass);


--
-- Name: outbound_clicks id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbound_clicks ALTER COLUMN id SET DEFAULT nextval('public.outbound_clicks_id_seq'::regclass);


--
-- Name: partner_links id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.partner_links ALTER COLUMN id SET DEFAULT nextval('public.partner_links_id_seq'::regclass);


--
-- Name: routes id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes ALTER COLUMN id SET DEFAULT nextval('public.routes_id_seq'::regclass);


--
-- Name: saved_trains id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.saved_trains ALTER COLUMN id SET DEFAULT nextval('public.saved_trains_id_seq'::regclass);


--
-- Name: search_index id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.search_index ALTER COLUMN id SET DEFAULT nextval('public.search_index_id_seq'::regclass);


--
-- Name: service_calendar id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.service_calendar ALTER COLUMN id SET DEFAULT nextval('public.service_calendar_id_seq'::regclass);


--
-- Name: stations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stations ALTER COLUMN id SET DEFAULT nextval('public.stations_id_seq'::regclass);


--
-- Name: stop_times id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stop_times ALTER COLUMN id SET DEFAULT nextval('public.stop_times_id_seq'::regclass);


--
-- Name: trips id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips ALTER COLUMN id SET DEFAULT nextval('public.trips_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: compensation_rules compensation_rules_country_code_min_delay_minutes_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.compensation_rules
    ADD CONSTRAINT compensation_rules_country_code_min_delay_minutes_key UNIQUE (country_code, min_delay_minutes);


--
-- Name: compensation_rules compensation_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.compensation_rules
    ADD CONSTRAINT compensation_rules_pkey PRIMARY KEY (id);


--
-- Name: countries countries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT countries_pkey PRIMARY KEY (code);


--
-- Name: delay_observations delay_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delay_observations
    ADD CONSTRAINT delay_observations_pkey PRIMARY KEY (id);


--
-- Name: outbound_clicks outbound_clicks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbound_clicks
    ADD CONSTRAINT outbound_clicks_pkey PRIMARY KEY (id);


--
-- Name: partner_links partner_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.partner_links
    ADD CONSTRAINT partner_links_pkey PRIMARY KEY (id);


--
-- Name: routes routes_country_gtfs_route_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_country_gtfs_route_id_key UNIQUE (country_code, gtfs_route_id);


--
-- Name: routes routes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_pkey PRIMARY KEY (id);


--
-- Name: saved_trains saved_trains_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.saved_trains
    ADD CONSTRAINT saved_trains_pkey PRIMARY KEY (id);


--
-- Name: saved_trains saved_trains_user_id_country_code_identifier_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.saved_trains
    ADD CONSTRAINT saved_trains_user_id_country_code_identifier_key UNIQUE (user_id, country_code, identifier);


--
-- Name: search_index search_index_country_code_kind_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.search_index
    ADD CONSTRAINT search_index_country_code_kind_name_key UNIQUE (country_code, kind, name);


--
-- Name: search_index search_index_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.search_index
    ADD CONSTRAINT search_index_pkey PRIMARY KEY (id);


--
-- Name: service_calendar service_calendar_country_code_service_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.service_calendar
    ADD CONSTRAINT service_calendar_country_code_service_id_key UNIQUE (country_code, service_id);


--
-- Name: service_calendar service_calendar_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.service_calendar
    ADD CONSTRAINT service_calendar_pkey PRIMARY KEY (id);


--
-- Name: stations stations_country_gtfs_stop_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stations
    ADD CONSTRAINT stations_country_gtfs_stop_id_key UNIQUE (country_code, gtfs_stop_id);


--
-- Name: stations stations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stations
    ADD CONSTRAINT stations_pkey PRIMARY KEY (id);


--
-- Name: stop_times stop_times_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stop_times
    ADD CONSTRAINT stop_times_pkey PRIMARY KEY (id);


--
-- Name: trips trips_country_gtfs_trip_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips
    ADD CONSTRAINT trips_country_gtfs_trip_id_key UNIQUE (country_code, gtfs_trip_id);


--
-- Name: trips trips_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips
    ADD CONSTRAINT trips_pkey PRIMARY KEY (id);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: idx_delay_obs_cancelled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delay_obs_cancelled ON public.delay_observations USING btree (country_code, observed_date) WHERE is_cancelled;


--
-- Name: idx_delay_obs_country; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delay_obs_country ON public.delay_observations USING btree (country_code);


--
-- Name: idx_delay_obs_country_arrival; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delay_obs_country_arrival ON public.delay_observations USING btree (country_code, actual_arrival);


--
-- Name: idx_delay_obs_live_trip_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delay_obs_live_trip_id ON public.delay_observations USING btree (country_code, live_trip_id);


--
-- Name: idx_delay_obs_route_country; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delay_obs_route_country ON public.delay_observations USING btree (route_name, country_code) INCLUDE (delay_minutes, actual_arrival);


--
-- Name: idx_delay_obs_station_country; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delay_obs_station_country ON public.delay_observations USING btree (station_id, country_code) INCLUDE (delay_minutes, actual_arrival);


--
-- Name: idx_delay_obs_trip_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delay_obs_trip_id ON public.delay_observations USING btree (trip_id);


--
-- Name: idx_delay_obs_trip_stop; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_delay_obs_trip_stop ON public.delay_observations USING btree (country_code, trip_id, stop_sequence) WHERE (stop_sequence IS NOT NULL);


--
-- Name: idx_outbound_clicks_link; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_outbound_clicks_link ON public.outbound_clicks USING btree (partner_link_id, clicked_at);


--
-- Name: idx_partner_links_scope; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_partner_links_scope ON public.partner_links USING btree (country_code, scope, scope_key) WHERE active;


--
-- Name: idx_routes_country_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_routes_country_name ON public.routes USING btree (country_code, name);


--
-- Name: idx_saved_trains_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_saved_trains_user ON public.saved_trains USING btree (user_id);


--
-- Name: idx_search_index_country_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_search_index_country_kind ON public.search_index USING btree (country_code, kind, observations DESC);


--
-- Name: idx_search_index_text_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_search_index_text_trgm ON public.search_index USING gin (search_text public.gin_trgm_ops);


--
-- Name: idx_service_calendar_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_service_calendar_lookup ON public.service_calendar USING btree (country_code, service_id);


--
-- Name: idx_stations_country_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stations_country_name ON public.stations USING btree (country_code, name);


--
-- Name: idx_stop_times_station; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stop_times_station ON public.stop_times USING btree (station_id);


--
-- Name: idx_stop_times_trip_seq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_stop_times_trip_seq ON public.stop_times USING btree (trip_id, stop_sequence);


--
-- Name: idx_trips_country; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trips_country ON public.trips USING btree (country_code);


--
-- Name: idx_trips_country_service; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trips_country_service ON public.trips USING btree (country_code, service_id);


--
-- Name: idx_trips_country_short_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trips_country_short_name ON public.trips USING btree (country_code, trip_short_name);


--
-- Name: compensation_rules compensation_rules_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.compensation_rules
    ADD CONSTRAINT compensation_rules_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: delay_observations delay_observations_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delay_observations
    ADD CONSTRAINT delay_observations_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: delay_observations delay_observations_station_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delay_observations
    ADD CONSTRAINT delay_observations_station_id_fkey FOREIGN KEY (station_id) REFERENCES public.stations(id);


--
-- Name: delay_observations delay_observations_trip_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delay_observations
    ADD CONSTRAINT delay_observations_trip_id_fkey FOREIGN KEY (trip_id) REFERENCES public.trips(id);


--
-- Name: outbound_clicks outbound_clicks_partner_link_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbound_clicks
    ADD CONSTRAINT outbound_clicks_partner_link_id_fkey FOREIGN KEY (partner_link_id) REFERENCES public.partner_links(id);


--
-- Name: outbound_clicks outbound_clicks_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbound_clicks
    ADD CONSTRAINT outbound_clicks_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: partner_links partner_links_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.partner_links
    ADD CONSTRAINT partner_links_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: routes routes_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: saved_trains saved_trains_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.saved_trains
    ADD CONSTRAINT saved_trains_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: saved_trains saved_trains_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.saved_trains
    ADD CONSTRAINT saved_trains_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: search_index search_index_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.search_index
    ADD CONSTRAINT search_index_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: service_calendar service_calendar_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.service_calendar
    ADD CONSTRAINT service_calendar_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: stations stations_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stations
    ADD CONSTRAINT stations_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: stop_times stop_times_station_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stop_times
    ADD CONSTRAINT stop_times_station_id_fkey FOREIGN KEY (station_id) REFERENCES public.stations(id);


--
-- Name: stop_times stop_times_trip_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stop_times
    ADD CONSTRAINT stop_times_trip_id_fkey FOREIGN KEY (trip_id) REFERENCES public.trips(id);


--
-- Name: trips trips_country_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips
    ADD CONSTRAINT trips_country_code_fkey FOREIGN KEY (country_code) REFERENCES public.countries(code);


--
-- Name: trips trips_route_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips
    ADD CONSTRAINT trips_route_id_fkey FOREIGN KEY (route_id) REFERENCES public.routes(id);


--
-- Name: TABLE compensation_rules; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.compensation_rules TO trainuser;


--
-- Name: SEQUENCE compensation_rules_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.compensation_rules_id_seq TO trainuser;


--
-- Name: TABLE countries; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.countries TO trainuser;


--
-- Name: TABLE delay_observations; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.delay_observations TO trainuser;


--
-- Name: SEQUENCE delay_observations_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.delay_observations_id_seq TO trainuser;


--
-- Name: TABLE outbound_clicks; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.outbound_clicks TO trainuser;


--
-- Name: SEQUENCE outbound_clicks_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.outbound_clicks_id_seq TO trainuser;


--
-- Name: TABLE partner_links; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.partner_links TO trainuser;


--
-- Name: SEQUENCE partner_links_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.partner_links_id_seq TO trainuser;


--
-- Name: TABLE routes; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.routes TO trainuser;


--
-- Name: SEQUENCE routes_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.routes_id_seq TO trainuser;


--
-- Name: TABLE saved_trains; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.saved_trains TO trainuser;


--
-- Name: SEQUENCE saved_trains_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.saved_trains_id_seq TO trainuser;


--
-- Name: TABLE search_index; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.search_index TO trainuser;


--
-- Name: SEQUENCE search_index_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.search_index_id_seq TO trainuser;


--
-- Name: TABLE service_calendar; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.service_calendar TO trainuser;


--
-- Name: SEQUENCE service_calendar_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.service_calendar_id_seq TO trainuser;


--
-- Name: TABLE stations; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.stations TO trainuser;


--
-- Name: SEQUENCE stations_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.stations_id_seq TO trainuser;


--
-- Name: TABLE stop_times; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.stop_times TO trainuser;


--
-- Name: SEQUENCE stop_times_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.stop_times_id_seq TO trainuser;


--
-- Name: TABLE trips; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.trips TO trainuser;


--
-- Name: SEQUENCE trips_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.trips_id_seq TO trainuser;


--
-- Name: TABLE users; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE public.users TO trainuser;


--
-- Name: SEQUENCE users_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public.users_id_seq TO trainuser;


--
-- PostgreSQL database dump complete
--

\unrestrict Xkdj0joJolN3mkPg2I8pxfTTpQz0oQi5PvdEFkk5xEdop6qjIDdrVYH057xU7NJ

