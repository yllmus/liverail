-- Real, sourced EU rail passenger-rights compensation figures, researched
-- 2026-08-20 from each operator's own passenger-rights page. All four countries are EU members and Regulation (EU)
-- 2021/782 sets the baseline (60min=25%, 120min=50%, delay measured at
-- the destination station on the ticket) - AT/DE/PL all match this
-- baseline exactly per their own operators' published passenger-rights
-- pages. HU/MÁV is the one real deviation: its flat-rate compensation
-- kicks in at 30 minutes rather than 60, but only for premium/
-- reserved-seat trains specifically (not every HU service) - encoded in
-- `notes` rather than silently generalized to all HU trains, since that
-- would overclaim eligibility to a user. Idempotent (ON CONFLICT DO
-- UPDATE keyed on the existing UNIQUE(country_code, min_delay_minutes)).

INSERT INTO compensation_rules (country_code, min_delay_minutes, refund_pct, source_url, verified, notes) VALUES
('HU', 30, 25.00, 'https://www.mavcsoport.hu/mav-szemelyszallitas/gyik/kesesigazolas-karterites', true,
 'MÁV flat-rate compensation - premium/reserved-seat domestic trains only, not every HU service. 30-119 min = 25%. Payouts under 1000 HUF are only paid once combined with another claim.'),
('HU', 120, 50.00, 'https://www.mavcsoport.hu/mav-szemelyszallitas/gyik/kesesigazolas-karterites', true,
 'MÁV flat-rate compensation - premium/reserved-seat domestic trains only. 120+ min = 50%.'),
('AT', 60, 25.00, 'https://www.oebb.at/en/fragen-und-antworten/rechte-pflichten/fahrgastrechte', true,
 'ÖBB long-distance (Fernverkehr) tickets, EU Reg 2021/782 baseline. 60-119 min = 25%. Weekly/monthly pass holders instead get a flat EUR 1.50 per delay over 20 min.'),
('AT', 120, 50.00, 'https://www.oebb.at/en/fragen-und-antworten/rechte-pflichten/fahrgastrechte', true,
 'ÖBB long-distance (Fernverkehr) tickets. 120+ min = 50%.'),
('DE', 60, 25.00, 'https://www.bahn.de/en/booking-information/passenger-rights', true,
 'Deutsche Bahn, EU Reg 2021/782 baseline. 60-119 min = 25%.'),
('DE', 120, 50.00, 'https://www.bahn.de/en/booking-information/passenger-rights', true,
 'Deutsche Bahn. 120+ min = 50%.'),
('PL', 60, 25.00, 'https://www.intercity.pl/en/dokumenty/wniosek_miedzynarodowy.pdf', true,
 'PKP Intercity, EU Reg 2021/782 baseline. Delay measured at the destination station printed on the ticket, not an intermediate stop. 60-119 min = 25%. Domestic claim window 12 months, international 1 month.'),
('PL', 120, 50.00, 'https://www.intercity.pl/en/dokumenty/wniosek_miedzynarodowy.pdf', true,
 'PKP Intercity. 120+ min = 50%. Not payable if the delay is due to force majeure (severe weather, flooding, etc.).')
ON CONFLICT (country_code, min_delay_minutes) DO UPDATE SET
  refund_pct = EXCLUDED.refund_pct, source_url = EXCLUDED.source_url,
  verified = EXCLUDED.verified, notes = EXCLUDED.notes;
