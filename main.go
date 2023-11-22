package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type OrderResponse struct {
	Data  OrderData `json:"data"`
	Error string    `json:"error"`
}

type OrderData struct {
	OrderID      int64  `json:"order_id"`
	Number       string `json:"number"`
	OrderStateID int    `json:"order_state_id"`
	Items        []Item `json:"items"`
}

type Item struct {
	ID       int64 `json:"id"`
	ItemID   int64 `json:"item_id"`
	Quantity int   `json:"quantity"`
}

const reqTemplate = `
{
    "client_id": %d,
    "client_loyalty_state_id": 4,
    "client_legal": false,
    "payment_type_id": 3,
    "payment_type_prepay": true,
    "payment_type_online": true,
    "address_map": {
        "key_1": {
            "address": {
                "address_schema_version": 2,
                "coordinate_precision_type": "House",
                "coordinate_precision_type_id": 0,
                "contractor_id": 3097337,
                "address_tail": "г. Москва, наб. Пресненская, д. 10 блок C",
                "address_uid": "0c5b2444-70a0-4932-980c-b4dc0d3f02b5",
                "addressee": "Maxine Hansen Петрович",
                "apartment": "38",
                "district": "Ryan Trail",
                "entrance": "Подъезд",
                "city_uid": "0c5b2444-70a0-4932-980c-b4dc0d3f02b5",
                "country": "Guadeloupe",
                "area_id": "2",
                "comment": "Заказ получит: Петрович Maxine 79165221787",
                "city": "Bowling Green",
                "first_name": "Maxine",
                "floor": "14",
                "house": "69",
                "intercom": "Mobile phone",
                "is_carpass_required": false,
                "is_pick_point": false,
                "is_verified": false,
                "last_name": "Hansen",
                "latitude": "55.776163",
                "longitude": "37.538817",
                "middle_name": "Петрович",
                "parent_address_id": 0,
                "phone": "79165221787",
                "point_id": 1,
                "region": "New Hampshire",
                "street": "Maggie Courts",
                "visible": true,
                "zip_code": "60253-9675",
                "gift_sender": {}
            }
        }
    },
    "delivery_attributes": [
        {
            "id": 1,
            "delivery_date_begin": "2023-11-22T11:09:39.942Z",
            "delivery_date_end": "2023-11-22T11:09:39.942Z",
            "delivery_additional_payment": 0
        }
    ],
    "delivery_price": 300,
    "comment": "From supply chain QA - comment",
    "client_tariff_zone_id": 16780,
    "client_delivery_variant_id": 11469,
    "delivery_variant_id": 11469,
    "client_delivery_variant": "From supply chain QA - delivery",
    "delivery_choice_id": 1,
    "client_account_payment": 0,
    "client_exit_date": "2023-11-22T11:30:58.745Z",
    "mart_id": 1,
    "ip": "192.168.0.1",
    "is_cross_dock": false,
    "is_gift_order": false,
    "is_today_delivery": false,
    "merchant_id": 667,
    "application": "desktop",
    "tags": [],
    "items": [
        {
            "item_id": "140927810",
            "marketplace_seller_id": 120,
            "marketplace_seller_price": 300,
            "price": 300,
            "quantity": 1,
            "score_to_add": 0,
            "attributes": [],
            "client_score_value": 0,
            "discount": 0,
            "installment_price": 0,
            "is_cross_dock": false,
            "is_fresh": false,
            "is_revards_expiration": false,
            "item_discount_amount": 0,
            "src_price": 0,
            "weight": 5000
        }
    ],
    "is_test": false,
    "uuid": "1c61f340-bb08-4662-9503-043d18bd42da",
    "virtual_postings": [
        {
            "posting_suggested_number": 1,
            "address": "key_1",
            "physical_store_id": 96695,
            "shipment_date": "2023-11-22T11:30:58.745Z",
            "min_delivery_date": "2023-11-22T11:30:58.745Z",
            "max_delivery_date": "2023-11-22T11:30:58.745Z",
            "delivery_payment_for_time_slot": 0,
            "is_user_choice": true,
            "items": [
                {
                    "item_id": "140927810",
                    "quantity": 1,
                    "item_availability_id": 1,
                    "weight": 5000,
                    "width": 750,
                    "length": 450,
                    "height": 380,
                    "delivery_schema": "FBO",
                    "marketplace_seller_id": 120
                }
            ],
            "is_simulated": false,
            "seller_warehouse_id": "18978815247000",
            "shipping_provider_id": 1461294009000,
            "marketplace_seller_id": 120,
            "delivery_price": 300,
            "rezon_delivery_variant_id": 96695,
            "clearing_delivery_variant_id": "15708161816000",
            "delivery_schema": "FBO",
            "delivery_type": "PVZ",
            "delivery_variant_name": "Текстовое название доставки",
            "delivery_restrictions": {
                "max_price": 100,
                "max_weight": 100
            },
            "additional_info": [
                {
                    "code": "IsExpress",
                    "value": "1"
                },
                {
                    "code": "department",
                    "value": "Supermarket"
                },
                {
                    "code": "HasPackage",
                    "value": "0"
                }
            ],
            "tpl_integration_type": "Ozon",
            "time_slot_id": 1014000134414314
        }
    ],
    "marketing_client_order_marketing_actions": [],
    "client_order_external_spp_address_info": [],
    "client_order_external_tarrification_info": [],
    "additional_info": [
        {
            "code": "ozonPreOrder",
            "value": "c9737375-1f20-474c-9e43-a3d3e7c6dff8"
        },
        {
            "code": "IsExpress",
            "value": "1"
        },
        {
            "code": "Department",
            "value": "Supermarket"
        }
    ],
    "marketing_addition_info": []
}
`

func main() {
	var goroutinesStr, postingsStr string

	fmt.Print("Сколько потоков (горутин)? ")
	_, err := fmt.Scanln(&goroutinesStr)
	if err != nil {
		fmt.Println("Ошибка ввода:", err)
		return
	}
	goroutines, err := strconv.Atoi(goroutinesStr)
	if err != nil {
		fmt.Println("Неверный ввод:", err)
		return
	}

	// Запрашиваем количество постингов
	fmt.Print("Сколько нужно постингов? ")
	_, err = fmt.Scanln(&postingsStr)
	if err != nil {
		fmt.Println("Ошибка ввода:", err)
		return
	}
	postings, err := strconv.Atoi(postingsStr)
	if err != nil {
		fmt.Println("Неверный ввод:", err)
		return
	}

	c := resty.New().SetHeader("Content-Type", "application/json")

	// Запускаем горутины
	if err := runPostings(goroutines, postings, c); err != nil {
		fmt.Println("Произошла ошибка:", err)
	} else {
		fmt.Println("Все задачи успешно выполнены")
	}
}

func runPostings(goroutines, postings int, c *resty.Client) error {
	g, ctx := errgroup.WithContext(context.Background())
	tasksChan := make(chan int, postings)

	for i := 0; i < postings; i++ {
		tasksChan <- i
	}
	close(tasksChan)

	for i := 0; i < goroutines; i++ {
		clientId := calculateClientID()
		g.Go(func() error {
			for {
				select {
				case _, ok := <-tasksChan:
					if !ok {
						return nil
					}
					if err := doPosting(ctx, c, clientId); err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	return g.Wait()
}

func calculateClientID() int64 {
	rand.Seed(time.Now().UnixNano())
	r := rand.Int63n(99999999-10000000+1) + 10000000
	fmt.Println(r)
	return r
}

func doPosting(ctx context.Context, c *resty.Client, clientId int64) error {
	reqBody := fmt.Sprintf(reqTemplate, clientId)
	resp, err := c.R().SetContext(ctx).
		SetBody(reqBody).
		Post("http://oms-go-api-web.oms.stg.s.o3.ru/v2/order/create")
	if err != nil {
		return err
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("/v2/order/create: %d :%s", resp.StatusCode(), resp.Body())
	}

	var or OrderResponse
	err = json.Unmarshal(resp.Body(), &or)
	if err != nil {
		return fmt.Errorf("unmarshall error: %w", err)
	}

	req2 := map[string]interface{}{
		"clientOrderDate": "2023-11-22T09:41:54.140Z",
		"productId":       or.Data.Items[0].ID,
		"price":           0,
		"number":          or.Data.Number,
		"clientOrderId":   or.Data.OrderID,
	}

	got, err := c.R().SetContext(ctx).SetContext(ctx).SetBody(req2).
		Post("http://oe-qa-fake-order-payment.stg.a.o3.ru/FakeOrderPayment")
	if err != nil {
		return err
	}

	if resp.StatusCode() != http.StatusOK {
		fmt.Printf("FakeOrderPayment: %d :%s", got.StatusCode(), got.Body())
	}

	return nil
}
