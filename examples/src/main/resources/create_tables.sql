CREATE TABLE IF NOT EXISTS `authors` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(50) COLLATE utf8_unicode_ci,
  `last_name` varchar(50) COLLATE utf8_unicode_ci,
  `email` varchar(100) COLLATE utf8_unicode_ci,
  `birthdate` date,
  `added` timestamp DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE IF NOT EXISTS `posts` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `author_id` int(11),
  `title` varchar(255) COLLATE utf8_unicode_ci,
  `description` varchar(500) COLLATE utf8_unicode_ci,
  `content` text COLLATE utf8_unicode_ci,
  `date` date,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE IF NOT EXISTS `author_posts` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(50) COLLATE utf8_unicode_ci,
  `last_name` varchar(50) COLLATE utf8_unicode_ci,
  `email` varchar(100) COLLATE utf8_unicode_ci,
  `posts` bigint(32),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
