SET binary_output_format = 'hex';
SELECT to_binary('xyz'), [to_binary('xyz'), NULL], (to_binary('xyz'), to_binary('abc')), map(['key'], [to_binary('xyz')]);

SET binary_output_format = 'base64';
SELECT to_binary('xyz'), [to_binary('xyz'), NULL], (to_binary('xyz'), to_binary('abc')), map(['key'], [to_binary('xyz')]);

SET binary_output_format = 'utf-8';
SELECT to_binary('xyz'), [to_binary('xyz'), NULL], (to_binary('xyz'), to_binary('abc')), map(['key'], [to_binary('xyz')]);

SET binary_output_format = 'utf-8-lossy';
SELECT to_binary('xyz'), [to_binary('xyz'), NULL], (to_binary('xyz'), to_binary('abc')), map(['key'], [to_binary('xyz')]);
