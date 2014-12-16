CREATE TABLE IF NOT EXISTS `nber_category` (
          `id` varchar(20) NOT NULL,
          `title` varchar(512) DEFAULT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        
        CREATE TABLE IF NOT EXISTS `nber_subcategory` (
          `id` varchar(20) NOT NULL,
          `title` varchar(512) DEFAULT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        
        CREATE TABLE IF NOT EXISTS `nber` (
          `uuid` varchar(36) NOT NULL,
          `patent_id` varchar(20) DEFAULT NULL,
          `category_id` varchar(20) DEFAULT NULL,
          `subcategory_id` varchar(20) DEFAULT NULL,
          PRIMARY KEY (`uuid`),
          KEY `patent_id` (`patent_id`),
          KEY `category_id` (`category_id`),
          KEY `subcategory_id` (`subcategory_id`),
          CONSTRAINT `nber_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `patent` (`id`),
          CONSTRAINT `nber_ibfk_2` FOREIGN KEY (`category_id`) REFERENCES `nber_category` (`id`),
          CONSTRAINT `nber_ibfk_3` FOREIGN KEY (`subcategory_id`) REFERENCES `nber_subcategory` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;