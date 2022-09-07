--@test: encode - encode hex
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, encode(input_string, 'hex', 'ascii') AS ASCII, encode(input_string, 'hex', 'utf8') as UTF8, encode(input_string, 'hex', 'base64') as BASE64 FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('4578616d706C6521');
INSERT INTO `TEST` (input_string) VALUES ('ce95cebbcebbceacceb4ceb1');
INSERT INTO `TEST` (input_string) VALUES ('c39c6265726d656e736368');
INSERT INTO `TEST` (input_string) VALUES (NULL);
INSERT INTO `TEST` (input_string) VALUES ('0x4578616d706C6521');
INSERT INTO `TEST` (input_string) VALUES ('X''4578616d706C6521''');
INSERT INTO `TEST` (input_string) VALUES ('x''4578616d706C6521''');
INSERT INTO `TEST` (input_string) VALUES ('0x');
INSERT INTO `TEST` (input_string) VALUES ('X''''');
INSERT INTO `TEST` (input_string) VALUES ('x''''');
INSERT INTO `TEST` (input_string) VALUES ('0x0x');
INSERT INTO `TEST` (input_string) VALUES ('X''');
INSERT INTO `TEST` (input_string) VALUES ('x''4578616d706C6521');
INSERT INTO `TEST` (input_string) VALUES ('x''578616d706C6521''');
INSERT INTO `TEST` (input_string) VALUES ('0x578616d706C6521');
INSERT INTO `TEST` (input_string) VALUES ('578616d706C6521');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('Example!', 'Example!', 'RXhhbXBsZSE=');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('������������', 'Ελλάδα', 'zpXOu867zqzOtM6x');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('��bermensch', 'Übermensch', 'w5xiZXJtZW5zY2g=');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('Example!', 'Example!', 'RXhhbXBsZSE=');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('Example!', 'Example!', 'RXhhbXBsZSE=');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('Example!', 'Example!', 'RXhhbXBsZSE=');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('', '', '');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('', '', '');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('', '', '');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES ('xample!', 'xample!', 'BXhhbXBsZSE=');
ASSERT VALUES `OUTPUT` (ASCII, UTF8, BASE64) VALUES (NULL, NULL, NULL);

--@test: encode - encode ascii
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, encode(input_string, 'ascii', 'hex') AS HEX, encode(input_string, 'ascii', 'utf8') as UTF8, encode(input_string, 'ascii', 'base64') as BASE64 FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('Example!');
INSERT INTO `TEST` (input_string) VALUES ('Ελλάδα');
INSERT INTO `TEST` (input_string) VALUES (NULL);
ASSERT VALUES `OUTPUT` (HEX, UTF8, BASE64) VALUES ('4578616d706c6521', 'Example!', 'RXhhbXBsZSE=');
ASSERT VALUES `OUTPUT` (HEX, UTF8, BASE64) VALUES ('3f3f3f3f3f3f', '??????', 'Pz8/Pz8/');
ASSERT VALUES `OUTPUT` (HEX, UTF8, BASE64) VALUES (NULL, NULL, NULL);

--@test: encode - encode base64
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, encode(input_string, 'base64', 'hex') AS HEX, encode(input_string, 'base64', 'utf8') as UTF8, encode(input_string, 'base64', 'ascii') as ASCII FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('RXhhbXBsZSE=');
INSERT INTO `TEST` (input_string) VALUES ('zpXOu867zqzOtM6x');
INSERT INTO `TEST` (input_string) VALUES (NULL);
ASSERT VALUES `OUTPUT` (HEX, UTF8, ASCII) VALUES ('4578616d706c6521', 'Example!', 'Example!');
ASSERT VALUES `OUTPUT` (HEX, UTF8, ASCII) VALUES ('ce95cebbcebbceacceb4ceb1', 'Ελλάδα', '������������');
ASSERT VALUES `OUTPUT` (HEX, UTF8, ASCII) VALUES (NULL, NULL, NULL);

--@test: encode - encode null
CREATE STREAM TEST (K STRING KEY, input_string VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, encode(input_string, 'base64', 'null') AS HEX, encode(input_string, 'null', 'utf8') as UTF8, encode(input_string, 'null', 'ascii') as ASCII FROM TEST;
INSERT INTO `TEST` (input_string) VALUES ('RXhhbXBsZSE=');
INSERT INTO `TEST` (input_string) VALUES (NULL);
ASSERT VALUES `OUTPUT` (HEX, UTF8, ASCII) VALUES (NULL, NULL, NULL);
ASSERT VALUES `OUTPUT` (HEX, UTF8, ASCII) VALUES (NULL, NULL, NULL);

