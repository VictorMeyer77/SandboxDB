use crate::storage::page::encoding::Encoding;
use crate::storage::page::page_error::PageError;
use crate::storage::page::page_header::PageHeader;
use crate::storage::page::slot::Slot;
use crate::storage::page::tuple::Tuple;
use crate::storage::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    schema: Schema,
    header: PageHeader,
    slots: Vec<Slot>,
    tuples: Vec<Tuple>,
}

impl Page {
    pub fn build(
        schema: &Schema,
        page_size: u32,
        version: [u8; 3],
        compression: u8,
    ) -> Result<Page, PageError> {
        let header = PageHeader::build(page_size, version, compression);
        Ok(Page {
            schema: schema.clone(),
            header,
            slots: vec![],
            tuples: vec![],
        })
    }

    fn get_free_slots(&self) -> Result<Vec<Slot>, PageError> {
        let used_bytes: Vec<Vec<u32>> = self
            .slots
            .iter()
            .map(|slot| (slot.offset..(slot.offset + slot.length)).collect())
            .collect();
        let used_bytes: Vec<u32> = used_bytes.concat();
        let free_bytes: Vec<u32> = ((45 + self.slots.len() as u32 * 8)..self.header.page_size)
            .into_iter()
            .filter(|byte| !used_bytes.contains(byte))
            .collect();
        let chunks: Vec<u32> = free_bytes
            .iter()
            .enumerate()
            .filter(|(k, _)| {
                if k > &(0usize) && k < &(free_bytes.len() - 1) {
                    free_bytes[*k] - free_bytes[*k - 1] != 1
                        || free_bytes[*k + 1] - free_bytes[*k] != 1
                } else {
                    true
                }
            })
            .map(|(_, v)| *v)
            .collect();
        let chunks: Vec<Slot> = chunks
            .chunks(2)
            .map(|chunk| Slot::build(chunk[0], chunk[1] - chunk[0] + 1))
            .collect();
        Ok(chunks)
    }

    pub fn insert(&mut self, nulls: &[u8], data: &[u8]) -> Result<(), PageError> {
        let tuple = Tuple::build(&self.schema, nulls, data)?;
        let tuple_size = tuple.get_bytes_size();
        let mut free_slots: Vec<Slot> = self
            .get_free_slots()?
            .into_iter()
            .filter(|slot| slot.length as usize > tuple_size)
            .collect();
        if free_slots.is_empty() {
            Err(PageError::PageOverflow(
                "Insertion failed, no more place on this page.".to_string(),
            ))
        } else {
            free_slots.sort_by_key(|slot| slot.length);
            let slot = Slot::build(
                free_slots.first().unwrap().offset + free_slots.first().unwrap().length
                    - tuple_size as u32,
                tuple_size as u32,
            );
            self.slots.push(slot);
            self.tuples.push(tuple);
            self.header.slots += 1;
            Ok(())
        }
    }

    pub fn delete_by_slots(&mut self, slots: &[Slot]) -> Result<usize, PageError> {
        let mut indexes: Vec<usize> = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, v)| slots.contains(v))
            .map(|(k, _)| k)
            .collect();
        indexes.sort_by(|a, b| b.cmp(a));
        for i in &indexes {
            self.slots.remove(*i);
            self.tuples.remove(*i);
            self.header.slots -= 1;
        }
        Ok(indexes.len())
    }

    pub fn update_by_slot(
        &mut self,
        slot: Slot,
        nulls: &[u8],
        data: &[u8],
    ) -> Result<(), PageError> {
        self.delete_by_slots(&[slot])?;
        self.insert(nulls, data)?;
        Ok(())
    }

    pub fn read_by_slots(&self, slots: &[Slot]) -> Result<Vec<Tuple>, PageError> {
        let tuples: Vec<Tuple> = self
            .tuples
            .iter()
            .enumerate()
            .zip(self.slots.iter().enumerate())
            .filter(|((_, _), (_, s))| slots.contains(s))
            .map(|((_, t), (_, _))| t.clone())
            .collect();
        Ok(tuples)
    }
}

impl Encoding<Page> for Page {
    fn as_bytes(&self) -> Vec<u8> {
        let mut concat_bytes: Vec<u8> = Vec::new();
        concat_bytes.extend_from_slice(&self.header.as_bytes());
        self.slots
            .iter()
            .for_each(|slot| concat_bytes.extend_from_slice(&slot.as_bytes()));
        let tuple_max_size = concat_bytes.len();
        let mut tuples: Vec<u8> = vec![0; self.header.page_size as usize - tuple_max_size];
        self.slots.iter().enumerate().for_each(|(k, v)| {
            tuples.splice(
                v.offset as usize - tuple_max_size
                    ..(v.offset + v.length - tuple_max_size as u32) as usize,
                self.tuples[k].as_bytes(),
            );
        });
        concat_bytes.extend_from_slice(&tuples);
        concat_bytes
    }

    fn from_bytes(bytes: &[u8], schema: Option<&Schema>) -> Result<Page, PageError> {
        let schema = schema.ok_or(PageError::MissingSchema)?;
        let header = PageHeader::from_bytes(&bytes[..45], None)?;
        let slots: Vec<Slot> = bytes[45..(45 + (header.slots as usize * 8))]
            .chunks(8)
            .map(|chunk| Slot::from_bytes(&chunk, None).unwrap())
            .collect();
        let tuples = slots
            .iter()
            .map(|slot| {
                Tuple::from_bytes(
                    &bytes[slot.offset as usize..(slot.offset + slot.length) as usize],
                    Some(&schema),
                )
                .unwrap()
            })
            .collect();
        Ok(Page {
            schema: schema.clone(),
            header,
            slots,
            tuples,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::tuple_header::TupleHeader;

    fn get_test_schema() -> Schema {
        Schema::from_str("id BIGINT, cost FLOAT, available BOOLEAN, date TIMESTAMP").unwrap()
    }

    fn get_test_page() -> Page {
        let mut page = Page::build(&get_test_schema(), 500, [10, 28, 45], 1).unwrap();
        page.slots = vec![
            Slot::build(462, 38),
            Slot::build(350, 22),
            Slot::build(250, 30),
        ];
        page.tuples = vec![
            Tuple::build(&get_test_schema(), &[0; 4], &[2; 33]).unwrap(),
            Tuple::build(&get_test_schema(), &[1, 0, 0, 0], &[8; 17]).unwrap(),
            Tuple::build(&get_test_schema(), &[0, 0, 0, 1], &[65; 25]).unwrap(),
        ];
        page.header.slots = 3;
        page
    }

    fn get_test_page_bytes() -> Vec<u8> {
        vec![
            244, 1, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 28, 45, 0, 1, 206, 1, 0, 0, 38, 0, 0, 0, 94, 1,
            0, 0, 22, 0, 0, 0, 250, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8, 8,
            8, 8, 8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        ]
    }

    #[test]
    fn as_bytes_should_convert_page() {
        let bytes = get_test_page().as_bytes();
        assert_eq!(bytes, get_test_page_bytes());
    }

    #[test]
    fn from_bytes_should_convert_bytes() {
        assert_eq!(
            get_test_page(),
            Page::from_bytes(&get_test_page_bytes(), Some(&get_test_schema())).unwrap()
        );
    }

    #[test]
    fn get_free_slots_should_return_all_empty_slots() {
        assert_eq!(
            get_test_page().get_free_slots().unwrap(),
            vec![
                Slot {
                    offset: 69,
                    length: 181,
                },
                Slot {
                    offset: 280,
                    length: 70,
                },
                Slot {
                    offset: 372,
                    length: 90,
                },
            ]
        );
    }

    #[test]
    fn insert_should_append_tuple() {
        let mut page = get_test_page();
        page.insert(&[1, 1, 0, 1], &[1]).unwrap();
        page.insert(&[0, 0, 0, 0], &[32; 33]).unwrap();
        page.insert(&[0, 0, 0, 0], &[18; 33]).unwrap();
        assert_eq!(page.get_bytes_size(), 500);
        assert_eq!(
            page,
            Page::from_bytes(&page.as_bytes(), Some(&get_test_schema())).unwrap()
        );
        assert_eq!(
            page.get_free_slots().unwrap(),
            vec![
                Slot {
                    offset: 93,
                    length: 157,
                },
                Slot {
                    offset: 280,
                    length: 26,
                },
                Slot {
                    offset: 372,
                    length: 52,
                },
            ]
        )
    }

    #[test]
    #[should_panic]
    fn insert_should_panic_if_full_page() {
        let mut page = get_test_page();
        for _ in 0..7 {
            page.insert(&[0, 0, 0, 0], &[18; 33]).unwrap();
        }
    }

    #[test]
    fn delete_by_slots_should_remove_tuples() {
        let mut page = get_test_page();
        page.insert(&[1, 1, 0, 1], &[1]).unwrap();
        page.insert(&[0, 0, 0, 0], &[32; 33]).unwrap();
        page.insert(&[0, 0, 0, 0], &[18; 33]).unwrap();
        let count = page
            .delete_by_slots(&[
                Slot::build(344, 6),
                Slot::build(306, 38),
                Slot::build(424, 38),
                Slot::build(27, 11),
            ])
            .unwrap();
        assert_eq!(page, get_test_page());
        assert_eq!(count, 3);
    }

    #[test]
    fn update_by_slot_should_replace_tuple() {
        let mut page = get_test_page();
        page.update_by_slot(Slot::build(250, 30), &[1, 1, 0, 1], &[1])
            .unwrap();
        assert_eq!(
            vec![
                Slot {
                    offset: 69,
                    length: 281,
                },
                Slot {
                    offset: 372,
                    length: 84,
                },
            ],
            page.get_free_slots().unwrap()
        );
        assert_eq!(page.tuples.last().unwrap().as_bytes(), [0, 1, 1, 0, 1, 1]);
    }

    #[test]
    fn read_by_slots_should_return_tuples() {
        let mut page = get_test_page();
        page.insert(&[1, 1, 0, 1], &[1]).unwrap();
        page.insert(&[0, 0, 0, 0], &[32; 33]).unwrap();
        page.insert(&[0, 0, 0, 0], &[18; 33]).unwrap();
        let tuples = page
            .read_by_slots(&[
                Slot::build(344, 6),
                Slot::build(350, 22),
                Slot::build(27, 11),
            ])
            .unwrap();
        assert_eq!(
            tuples,
            vec![
                Tuple {
                    header: TupleHeader {
                        visibility: 0,
                        nulls: [1, 0, 0, 0].to_vec(),
                    },
                    data: [8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8].to_vec(),
                },
                Tuple {
                    header: TupleHeader {
                        visibility: 0,
                        nulls: [1, 1, 0, 1].to_vec(),
                    },
                    data: [1].to_vec(),
                },
            ]
        );
    }
}
