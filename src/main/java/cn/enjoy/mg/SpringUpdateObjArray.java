package cn.enjoy.mg;


import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import javax.annotation.Resource;

import com.mongodb.client.result.UpdateResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.Update.PushOperatorBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import cn.enjoy.entity.Comment;
import cn.enjoy.entity.Doc;
import cn.enjoy.entity.User;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class SpringUpdateObjArray {

    private static final Logger logger = LoggerFactory.getLogger(SpringUpdateObjArray.class);
	
	@Resource
	private MongoOperations tempelate;
	
	
	
    //--------------------------------------upsert demo--------------------------------------------------------------
    
    //测试upsert
    //db.users.update({"username":"cang"},{"$set":{"age":18}},{"upsert":true})
    @Test
    public void upsertTest(){
    	Query query = query(Criteria.where("username").is("cang"));
    	Update set = new Update().set("age", 18);
		UpdateResult upsert = tempelate.upsert(query, set, User.class);
    	System.out.println(upsert.getModifiedCount());
    	System.out.println(upsert.getUpsertedId());
    	
    }
    
    
    
    //测试unset,删除字段示例
    //db.users.updateMany({"username":"lison"},{"$unset":{"country":"","age":""}})
    @Test
    public void unsetTest(){
    	Query query = query(Criteria.where("username").is("lison"));
    	Update unset = new Update().unset("country").unset("age");

		UpdateResult upsert = tempelate.updateMulti(query, unset, User.class);
    	System.out.println(upsert.getModifiedCount());
    }
    
    //测试rename,更新字段名称示例
    //db.users.updateMany({"username":"lison"},{"$rename":{"lenght":"height", "username":"name"}})

    @Test
    public void renameTest(){
		Query query = query(Criteria.where("username").is("lison"));
		Update rename = new Update().rename("lenght", "height").rename("username", "name");
		UpdateResult upsert = tempelate.updateMulti(query, rename, User.class);
		System.out.println(upsert.getModifiedCount());

	}
    
    
    //测试pull pullAll,删除字符串数组中元素示例
//    db.users.updateMany({ "username" : "james"}, { "$pull" : { "favorites.movies" : [ "小电影2 " , "小电影3"]}})
//    db.users.updateMany({ "username" : "james"}, { "$pullAll" : { "favorites.movies" : [ "小电影2 " , "小电影3"]}})

    @Test
    public void pullAllTest(){
    	
		Query query = query(Criteria.where("username").is("james"));
		Update pull = new Update().pull("favorites.movies", Arrays.asList("小电影2 " , "小电影3"));
		UpdateResult upsert = tempelate.updateMulti(query, pull, User.class);
		System.out.println(upsert.getModifiedCount());
    	
    	
    	
		query = query(Criteria.where("username").is("james"));
		Update pullAll = new Update().pullAll("favorites.movies", new String[]{"小电影2 " , "小电影3"});
		upsert = tempelate.updateMulti(query, pullAll, User.class);
		System.out.println(upsert.getModifiedCount());
    }


    
    
    
    //--------------------------------------insert demo--------------------------------------------------------------
    
    
    //给james老师增加一条评论（$push）
    //db.users.updateOne({"username":"james"},
//                         {"$push":{"comments":{"author":"lison23",
//                                     "content":"ydddyyytttt",
//                                     "commentTime":ISODate("2019-01-06T00:00:00")}}})
    @Test
    public void addOneComment(){
    	Query query = query(Criteria.where("username").is("james"));
    	Comment comment = new Comment();
    	comment.setAuthor("lison23");
    	comment.setContent("ydddyyytttt");
    	comment.setCommentTime(getDate("2019-01-06"));
		Update push = new Update().push("comments", comment);
		UpdateResult updateFirst = tempelate.updateFirst(query, push, User.class);
		System.out.println(updateFirst.getModifiedCount());
    }
    
    
	//    给james老师批量新增两条评论（$push,$each）
//  db.users.updateOne({"username":"james"},     
//  	       {"$push":{"comments":
//  	                  {"$each":[{"author":"lison22","content":"yyyytttt","commentTime":ISODate("2019-02-06T00:00:00")},
//  	                            {"author":"lison23","content":"ydddyyytttt","commentTime":ISODate("2019-03-06T00:00:00")}]}}})
    @Test
    public void addManyComment(){
    	Query query = query(Criteria.where("username").is("james"));
    	Comment comment1 = new Comment();
    	comment1.setAuthor("lison55");
    	comment1.setContent("lison55lison55");
    	comment1.setCommentTime(getDate("2019-02-06"));
    	Comment comment2 = new Comment();
    	comment2.setAuthor("lison66");
    	comment2.setContent("lison66lison66");
    	comment2.setCommentTime(getDate("2019-03-06"));
		//Update push = new Update().pushAll("comments", new Comment[]{comment1,comment2});
        //Update push = new Update().push("comments", new Comment[]{comment1,comment2});
        Update push = new Update().push("comments").each(new Comment[]{comment1,comment2});
		UpdateResult updateFirst = tempelate.updateFirst(query, push, User.class);
		System.out.println(updateFirst.getModifiedCount());
    }
    
//  给james老师批量新增两条评论并对数组进行排序（$push,$eachm,$sort）
//  db.users.updateOne({"username":"james"}, 
//  	      {"$push": {"comments":
//  	                {"$each":[ {"author":"lison22","content":"yyyytttt","commentTime":ISODate("2019-04-06T00:00:00")},
//  	                           {"author":"lison23","content":"ydddyyytttt","commentTime":ISODate("2019-05-06T00:00:00")} ], 
//  	                  $sort: {"commentTime":-1} } } })
    @Test
    public void addManySortComment(){
    	Query query = query(Criteria.where("username").is("james"));
    	Comment comment1 = new Comment();
    	comment1.setAuthor("lison77");
    	comment1.setContent("lison55lison55");
    	comment1.setCommentTime(getDate("2019-04-06"));
    	Comment comment2 = new Comment();
    	comment2.setAuthor("lison88");
    	comment2.setContent("lison66lison66");
    	comment2.setCommentTime(getDate("2019-05-06"));
    	
    	
		Update update = new Update();
		PushOperatorBuilder pob = update.push("comments");
		pob.each(comment1,comment2);
		pob.sort(Sort.by(Direction.DESC, "commentTime"));
		
		System.out.println("---------------");
        UpdateResult updateFirst = tempelate.updateFirst(query, update,User.class);
		System.out.println(updateFirst.getModifiedCount());
    }
 
    //--------------------------------------delete demo--------------------------------------------------------------
 
//    删除lison1对james的所有评论 （批量删除）
//    db.users.update({"username":“james"},
//                               {"$pull":{"comments":{"author":"lison23"}}})

    @Test
    public void deleteByAuthorComment(){
    	Query query = query(Criteria.where("username").is("james"));
    	
    	Comment comment1 = new Comment();
    	comment1.setAuthor("lison55");


/*		BasicDBObject comment1 = new BasicDBObject ();
		comment1.put("author","lison23");*/

		Update pull = new Update().pull("comments",comment1);
        UpdateResult updateFirst = tempelate.updateFirst(query, pull, User.class);
		System.out.println(updateFirst.getModifiedCount());
    }
    
    
//    删除lison5对lison评语为“lison是苍老师的小迷弟”的评论（精确删除）
//    db.users.update({"username":"lison"},
//            {"$pull":{"comments":{"author":"lison5",
//                                  "content":"lison是苍老师的小迷弟"}}})
    @Test
    public void deleteByAuthorContentComment(){
    	Query query = query(Criteria.where("username").is("lison"));
    	Comment comment1 = new Comment();
    	comment1.setAuthor("lison5");
    	comment1.setContent("lison是苍老师的小迷弟");
		Update pull = new Update().pull("comments",comment1);
        UpdateResult updateFirst = tempelate.updateFirst(query, pull, User.class);
		System.out.println(updateFirst.getModifiedCount());
    }
    
    //--------------------------------------update demo--------------------------------------------------------------
//    db.users.updateMany({"username":"james","comments.author":"lison1"},
//            {"$set":{"comments.$.content":"xxoo",
//                        "comments.$.author":"lison10" }})
//    	含义：精确修改某人某一条精确的评论，如果有多个符合条件的数据，则修改最后一条数据。无法批量修改数组元素
  @Test
  public void updateOneComment(){
  		Query query = query(where("username").is("lison").and("comments.author").is("lison4"));
  		Update update = update("comments.$.content","xxoo").set("comments.$.author","lison11");
      UpdateResult updateFirst = tempelate.updateFirst(query, update, User.class);
		System.out.println(updateFirst.getModifiedCount());
  }

//--------------------------------------findandModify demo--------------------------------------------------------------

  
  
  //使用findandModify方法在修改数据同时返回更新前的数据或更新后的数据
//db.fam.findAndModify({query:{name:'morris1'}, 
//    update:{$inc:{age:1}}, 
//    'new':true});
  
	@Test
	public void findAndModifyTest(){
		Query query = query(where("name").is("morris1"));
		Update update = new Update().inc("age", 1);
		FindAndModifyOptions famo = FindAndModifyOptions.options().returnNew(true);
		
		Doc doc = tempelate.findAndModify(query, update,famo, Doc.class);
		System.out.println(doc.toString());
	}
	
	
	  private Date getDate(String string) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			
			Date parse=null;
			try {
				parse = sdf.parse(string);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return parse;
		}
	
}