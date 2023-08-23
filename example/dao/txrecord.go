package dao

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

type TXRecordPO struct {
	gorm.Model
	Status               string `gorm:"status"`
	ComponentTryStatuses string `gorm:"component_try_statuses"`
}

func (t TXRecordPO) TableName() string {
	return "tx_record"
}

type ComponentTryStatus struct {
	ComponentID string `json:"componentID"`
	TryStatus   string `json:"tryStatus"`
}

type TXRecordDAO struct {
	db *gorm.DB
}

func NewTXRecordDAO(db *gorm.DB) *TXRecordDAO {
	return &TXRecordDAO{
		db: db,
	}
}

func (t *TXRecordDAO) GetTXRecords(ctx context.Context, opts ...QueryOption) ([]*TXRecordPO, error) {
	db := t.db.WithContext(ctx).Model(&TXRecordPO{})
	for _, opt := range opts {
		db = opt(db)
	}

	var records []*TXRecordPO
	return records, db.Scan(&records).Error
}

func (t *TXRecordDAO) CreateTXRecord(ctx context.Context, record *TXRecordPO) (uint, error) {
	return record.ID, t.db.WithContext(ctx).Model(&TXRecordPO{}).Create(record).Error
}

func (t *TXRecordDAO) UpdateComponentStatus(ctx context.Context, id uint, componentID string, status string) error {
	return t.db.WithContext(ctx).Exec(fmt.Sprintf("update tx_record set component_try_statuses = json_replace(component_try_statuses,'$.%s.tryStatus','%s') where id = %d", componentID, status, id)).Error
}

func (t *TXRecordDAO) UpdateTXRecord(ctx context.Context, record *TXRecordPO) error {
	return t.db.WithContext(ctx).Updates(record).Error
}

func (t *TXRecordDAO) LockAndDo(ctx context.Context, id uint, do func(ctx context.Context, dao *TXRecordDAO, record *TXRecordPO) error) error {
	return t.db.Transaction(func(tx *gorm.DB) error {
		defer func() {
			if err := recover(); err != nil {
				tx.Rollback()
			}
		}()

		// 加写锁
		var record TXRecordPO
		if err := tx.Set("gorm:query_option", "FOR UPDATE").WithContext(ctx).First(&record, id).Error; err != nil {
			return err
		}

		txDAO := NewTXRecordDAO(tx)
		return do(ctx, txDAO, &record)
	})
}
