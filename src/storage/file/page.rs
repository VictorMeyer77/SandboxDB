use std::collections::HashMap;

use crc32fast::hash;
use serde::{Deserialize, Serialize};

use crate::storage::file::encoding::FileEncoding;
use crate::storage::file::error::Error;
use crate::storage::file::page_header::PageHeader;
use crate::storage::file::tuple::Tuple;
use crate::storage::schema::Schema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page {
    pub schema: Schema,
    pub header: PageHeader,
    pub tuples: HashMap<(u32, u32), Tuple>,
}

impl Page {
    pub fn build(schema: &Schema, page_size: u32, compression: u8) -> Result<Page, Error> {
        let header = PageHeader::build(page_size, compression);
        Ok(Page {
            schema: schema.clone(),
            header,
            tuples: HashMap::new(),
        })
    }

    fn get_free_slots(&self) -> Result<Vec<(u32, u32)>, Error> {
        let slots: Vec<(u32, u32)> = self.tuples.keys().cloned().collect();
        let mut slots: Vec<u32> = slots
            .into_iter()
            .flat_map(|(offset, length)| vec![offset, offset + length])
            .collect();
        slots.sort();
        if !slots.contains(&self.header.page_size) {
            slots.push(self.header.page_size);
        }
        if !slots.contains(&(14 + self.header.slots * 8)) {
            slots.insert(0, 14 + self.header.slots * 8)
        } else {
            slots.remove(0);
        }
        let free_slots: Vec<(u32, u32)> = slots
            .chunks_exact(2)
            .map(|chunk| (chunk[0], chunk[1] - chunk[0]))
            .filter(|(_, length)| *length > 0)
            .collect();
        Ok(free_slots)
    }

    pub fn insert(&mut self, nulls: &[u8], data: &[u8]) -> Result<(), Error> {
        let tuple = Tuple::build(&self.schema, nulls, data)?;
        let tuple_size = tuple.bytes_size() as u32;
        let mut free_slots: Vec<(u32, u32)> = self
            .get_free_slots()?
            .into_iter()
            .filter(|(_, length)| *length > tuple_size)
            .collect();
        if free_slots.is_empty() {
            Err(Error::PageOverflow(
                "Insertion failed, no more place on this page.".to_string(),
            ))
        } else {
            free_slots.sort_by_key(|(_, length)| *length);
            let slot: (u32, u32) = (
                free_slots.first().unwrap().0 + free_slots.first().unwrap().1 - tuple_size,
                tuple_size,
            );
            self.tuples.insert(slot, tuple);
            self.header.slots += 1;
            Ok(())
        }
    }

    pub fn delete_by_slots(&mut self, slots: &[(u32, u32)]) -> Result<(), Error> {
        for slot in slots {
            if self.tuples.remove(slot).is_some() {
                self.header.slots -= 1;
            }
        }
        Ok(())
    }

    pub fn update_by_slot(
        &mut self,
        slot: (u32, u32),
        nulls: &[u8],
        data: &[u8],
    ) -> Result<(), Error> {
        match self
            .tuples
            .insert(slot, Tuple::build(&self.schema, nulls, data)?)
        {
            None => Err(Error::InvalidSlot(slot)),
            Some(_) => Ok(()),
        }
    }

    pub fn read_by_slots(
        &self,
        slots: &[(u32, u32)],
    ) -> Result<HashMap<(u32, u32), &Tuple>, Error> {
        let mut tuples: HashMap<(u32, u32), &Tuple> = HashMap::new();
        for slot in slots {
            if let Some(tuple) = self.tuples.get(slot) {
                tuples.insert(*slot, tuple);
            }
        }
        Ok(tuples)
    }

    pub fn refresh_checksum(&mut self) {
        self.header.checksum = hash(&self.as_bytes()[17..])
    }

    pub fn valid_checksum(&self) -> bool {
        self.header.checksum == hash(&self.as_bytes()[17..])
    }
}

//impl FileEncoding for Page {}

impl FileEncoding for Page {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&self.header.as_bytes());
        let tuple_offset_start = concat_bytes.len() as u32 + self.header.slots * 8;
        let mut tuples: Vec<u8> = vec![0; (self.header.page_size - tuple_offset_start) as usize];
        self.tuples.iter().for_each(|(k, v)| {
            concat_bytes.extend_from_slice(&bincode::serialize(k).unwrap()); //*&[k.0.to_le_bytes(), k.1.to_le_bytes()*/].concat());
            tuples.splice(
                (k.0 - tuple_offset_start) as usize..(k.0 + k.1 - tuple_offset_start) as usize,
                v.as_bytes(), //self.tuples[k].as_bytes(),
            );
        });
        concat_bytes.extend_from_slice(&tuples);
        concat_bytes
    }

    fn from_bytes(bytes: &[u8], schema: Option<&Schema>) -> Result<Page, Error> {
        let header = PageHeader::from_bytes(&bytes[..14], None).unwrap();
        let slots: Vec<(u32, u32)> = bytes[14..(14 + (header.slots as usize * 8))]
            .chunks(8)
            .map(|chunk| {
                (
                    u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
                    u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
                )
            })
            .collect();
        let mut tuples: HashMap<(u32, u32), Tuple> = HashMap::new();
        slots.iter().for_each(|(offset, length)| {
            tuples.insert(
                (*offset, *length),
                Tuple::from_bytes(&bytes[*offset as usize..(offset + length) as usize], None)
                    .unwrap(),
            );
        });
        Ok(Page {
            schema: schema.unwrap().clone(),
            header,
            tuples,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::schema::encoding::SchemaEncoding;

    use super::*;

    fn get_test_schema() -> Schema {
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap()
    }

    pub fn get_test_page() -> Page {
        let mut page = Page::build(&get_test_schema(), 500, 1).unwrap();
        page.tuples.insert(
            (446, 54),
            Tuple::build(&get_test_schema(), &[0; 4], &[2; 33]).unwrap(),
        );
        page.tuples.insert(
            (334, 38),
            Tuple::build(&get_test_schema(), &[1, 0, 0, 0], &[8; 17]).unwrap(),
        );
        page.tuples.insert(
            (234, 46),
            Tuple::build(&get_test_schema(), &[0, 0, 0, 1], &[65; 25]).unwrap(),
        );
        page.header.slots = 3;
        page
    }

    fn get_test_page_bytes() -> Vec<u8> {
        vec![
            244, 1, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 1, 78, 1, 0, 0, 38, 0, 0, 0, 190, 1, 0, 0, 54,
            0, 0, 0, 234, 0, 0, 0, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 25, 0, 0, 0, 0, 0, 0, 0, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
            8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        ]
    }

    #[test]
    fn as_bytes_should_convert_page() {
        println!("{:?}", &get_test_page().as_bytes());
        assert_eq!(
            Page::from_bytes(&get_test_page().as_bytes(), Some(&get_test_schema())).unwrap(),
            get_test_page()
        );
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            get_test_page(),
            Page::from_bytes(&get_test_page_bytes(), Some(&get_test_schema())).unwrap()
        );
    }

    #[test]
    fn get_free_slots_should_return_empty_slots_case_normal() {
        let mut page = get_test_page();
        page.header.page_size += 10;
        assert_eq!(
            page.get_free_slots().unwrap(),
            vec![(38, 196), (280, 54), (372, 74), (500, 10)]
        );
    }

    #[test]
    fn get_free_slots_should_return_all_empty_slots_case_maximum() {
        assert_eq!(
            get_test_page().get_free_slots().unwrap(),
            vec![(38, 196,), (280, 54,), (372, 74,),]
        );
    }

    #[test]
    fn get_free_slots_should_return_empty_slots_case_none() {
        let mut page = Page::build(&get_test_schema(), 500, 1).unwrap();
        page.tuples.insert(
            (22, 478),
            Tuple::build(&get_test_schema(), &[0, 0, 0, 1], &[65; 25]).unwrap(),
        );
        page.header.slots += 1;
        assert_eq!(page.get_free_slots().unwrap(), vec![]);
    }

    #[test]
    fn get_free_slots_should_return_empty_slots_case_minimum() {
        let mut page = Page::build(&get_test_schema(), 500, 1).unwrap();
        page.tuples.insert(
            (38, 103),
            Tuple::build(&get_test_schema(), &[0, 0, 0, 1], &[65; 25]).unwrap(),
        );
        page.tuples.insert(
            (152, 25),
            Tuple::build(&get_test_schema(), &[0, 0, 0, 1], &[65; 25]).unwrap(),
        );
        page.tuples.insert(
            (200, 25),
            Tuple::build(&get_test_schema(), &[0, 0, 0, 1], &[65; 25]).unwrap(),
        );
        page.header.slots = 3;
        assert_eq!(
            page.get_free_slots().unwrap(),
            vec![(141, 11), (177, 23), (225, 275)]
        );
    }

    #[test]
    fn insert_should_append_tuple() {
        let mut page = get_test_page();
        page.insert(&[1, 1, 0, 1], &[1]).unwrap();
        page.insert(&[0, 0, 0, 0], &[32; 33]).unwrap();
        page.insert(&[0, 0, 0, 0], &[18; 33]).unwrap();
        assert_eq!(page.bytes_size(), 500);
        assert_eq!(
            page,
            Page::from_bytes(&page.as_bytes(), Some(&get_test_schema())).unwrap()
        );
        assert_eq!(
            page.get_free_slots().unwrap(),
            vec![(62, 118), (280, 32), (372, 20)]
        )
    }

    #[test]
    #[should_panic]
    fn insert_should_panic_if_full_page() {
        let mut page = get_test_page();
        for _ in 0..8 {
            page.insert(&[0, 0, 0, 0], &[18; 33]).unwrap();
        }
    }

    #[test]
    fn delete_by_slots_should_remove_tuples() {
        let mut page = get_test_page();
        page.insert(&[1, 1, 0, 1], &[1]).unwrap();
        page.insert(&[0, 0, 0, 0], &[32; 33]).unwrap();
        page.insert(&[0, 0, 0, 0], &[18; 33]).unwrap();
        println!("{:?}", page.tuples.keys());
        page.delete_by_slots(&[(180, 54), (334, 38), (312, 22), (27, 11)])
            .unwrap();
        println!("{:?}", page.tuples.keys());
        assert_eq!(page.tuples.len(), 3);
        assert_eq!(
            page.tuples.get(&(446, 54)).unwrap(),
            get_test_page().tuples.get(&(446, 54)).unwrap()
        );
        assert_eq!(
            page.tuples.get(&(234, 46)).unwrap(),
            get_test_page().tuples.get(&(234, 46)).unwrap()
        );
        assert_eq!(
            page.tuples.get(&(392, 54)).unwrap(),
            get_test_page().tuples.get(&(392, 54)).unwrap()
        );
    } //[(392, 54), (234, 46), (446, 54), (180, 54), (334, 38), (312, 22)]

    #[test]
    fn update_by_slot_should_replace_tuple() {
        let mut page = get_test_page();
        page.update_by_slot((250, 30), &[1, 1, 0, 1], &[1]).unwrap();
        assert_eq!(page.tuples[&(250, 30)].as_bytes(), [0, 1, 1, 0, 1, 1]);
    }

    #[test]
    #[should_panic]
    fn update_by_slot_should_panic_key_not_found() {
        let mut page = get_test_page();
        page.update_by_slot((251, 30), &[1, 1, 0, 1], &[1]).unwrap();
    }

    #[test]
    fn read_by_slots_should_return_tuples() {
        let mut page = get_test_page();
        page.insert(&[1, 1, 0, 1], &[1]).unwrap();
        page.insert(&[0, 0, 0, 0], &[32; 33]).unwrap();
        page.insert(&[0, 0, 0, 0], &[18; 33]).unwrap();
        let tuples = page
            .read_by_slots(&[(344, 6), (350, 22), (27, 11)])
            .unwrap();
        assert_eq!(
            *tuples[&(344, 6)],
            Tuple::build(&get_test_schema(), &[1, 1, 0, 1], &[1]).unwrap()
        );
        assert_eq!(
            *tuples[&(350, 22)],
            Tuple::build(
                &get_test_schema(),
                &[1, 0, 0, 0],
                &[8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8]
            )
            .unwrap()
        );
    }

    #[test]
    fn valid_checksum_should_control_integrity() {
        let mut page = get_test_page();
        assert!(!page.valid_checksum());
        page.refresh_checksum();
        assert!(page.valid_checksum());
    }
}
