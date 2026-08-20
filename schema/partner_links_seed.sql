-- Seeds partner_links with real, honest, working targets so the "book
-- tickets" / "file a claim" CTAs are functional today, not empty. These
-- are NOT affiliate-tagged (no Trainline/Omio partner account exists) - they
-- point at each operator's own official site.
-- Swapping in a real affiliate deep link later is a one-column UPDATE on
-- target_url; nothing else in the app needs to change, since /out/{id}
-- already redirects through this table.

INSERT INTO partner_links (country_code, scope, scope_key, kind, label, target_url, active) VALUES
('HU', 'global', NULL, 'affiliate_booking', 'Book tickets — MÁV', 'https://www.mavcsoport.hu/', true),
('AT', 'global', NULL, 'affiliate_booking', 'Book tickets — ÖBB', 'https://www.oebb.at/en', true),
('DE', 'global', NULL, 'affiliate_booking', 'Book tickets — DB', 'https://www.bahn.de/en', true),
('PL', 'global', NULL, 'affiliate_booking', 'Book tickets — PKP Intercity', 'https://www.intercity.pl/en', true),
('HU', 'global', NULL, 'compensation_partner', 'File a compensation claim — MÁV', 'https://www.mavcsoport.hu/mav-szemelyszallitas/gyik/kesesigazolas-karterites', true),
('AT', 'global', NULL, 'compensation_partner', 'File a compensation claim — ÖBB', 'https://www.oebb.at/en/fragen-und-antworten/rechte-pflichten/fahrgastrechte', true),
('DE', 'global', NULL, 'compensation_partner', 'File a compensation claim — DB', 'https://int.bahn.de/en/help/contact/passengers-rights-claim-form', true),
('PL', 'global', NULL, 'compensation_partner', 'File a compensation claim — PKP', 'https://www.intercity.pl/en/dokumenty/wniosek_miedzynarodowy.pdf', true)
ON CONFLICT DO NOTHING;
